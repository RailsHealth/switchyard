import pdfplumber
from lxml import etree
import os
import re
from app.extensions import mongo
from datetime import datetime
from bson import ObjectId
from io import StringIO

class FileParserService:
    @staticmethod
    def extract_text_from_file(file_path):
        try:
            _, extension = os.path.splitext(file_path)
            if extension.lower() == '.pdf':
                with pdfplumber.open(file_path) as pdf:
                    return "\n".join(page.extract_text() for page in pdf.pages)
            elif extension.lower() in ['.txt', '.xml', '.edi']:
                with open(file_path, 'r', encoding='utf-8') as file:
                    return file.read()
            else:
                raise ValueError(f"Unsupported file type: {extension}")
        except Exception as e:
            raise Exception(f"Failed to extract text from file: {str(e)}")

    @staticmethod
    def analyze_content(text):
        scores = {
            'CCDA': 0,
            'X12': 0,
            'Clinical Notes': 0
        }

        # CCDA checks
        if re.search(r'<ClinicalDocument.*>', text):
            scores['CCDA'] += 5
        if re.search(r'<component>.*<structuredBody>', text):
            scores['CCDA'] += 3
        if re.search(r'<code code=".*" codeSystem=".*" displayName=".*"', text):
            scores['CCDA'] += 2

        # X12 checks
        if re.search(r'ISA\*.*\*.*~', text):
            scores['X12'] += 5
        if re.search(r'GS\*.*\*.*~', text):
            scores['X12'] += 3
        if re.search(r'ST\*.*\*.*~', text):
            scores['X12'] += 2

        # Clinical Notes checks
        if re.search(r'(Patient Name|DOB|Date of Birth):', text, re.IGNORECASE):
            scores['Clinical Notes'] += 2
        if re.search(r'(Chief Complaint|History of Present Illness|Past Medical History):', text, re.IGNORECASE):
            scores['Clinical Notes'] += 3
        if len(text.split()) > 100 and scores['CCDA'] == 0 and scores['X12'] == 0:
            scores['Clinical Notes'] += 1

        detected_type = max(scores, key=scores.get)
        confidence = scores[detected_type] / sum(scores.values()) if sum(scores.values()) > 0 else 0

        return detected_type, confidence

    @staticmethod
    def parse_ccda(xml_string):
        try:
            root = etree.fromstring(xml_string)
            namespaces = {
                'cda': 'urn:hl7-org:v3',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }

            def extract_text(element):
                return ' '.join(element.xpath('.//text()'))

            parsed_data = {
                'document_type': root.xpath('//cda:code/@displayName', namespaces=namespaces)[0],
                'patient': {
                    'name': extract_text(root.xpath('//cda:patient/cda:name', namespaces=namespaces)[0]),
                    'gender': root.xpath('//cda:patient/cda:administrativeGenderCode/@code', namespaces=namespaces)[0],
                    'birth_date': root.xpath('//cda:patient/cda:birthTime/@value', namespaces=namespaces)[0],
                },
                'author': {
                    'name': extract_text(root.xpath('//cda:author//cda:assignedPerson/cda:name', namespaces=namespaces)[0]),
                    'organization': extract_text(root.xpath('//cda:author//cda:representedOrganization/cda:name', namespaces=namespaces)[0]),
                },
                'sections': {}
            }

            for section in root.xpath('//cda:component/cda:section', namespaces=namespaces):
                section_title = extract_text(section.xpath('cda:title', namespaces=namespaces)[0])
                section_text = extract_text(section.xpath('cda:text', namespaces=namespaces)[0])
                parsed_data['sections'][section_title] = section_text

            return parsed_data
        except Exception as e:
            return {"error": f"Failed to parse CCDA: {str(e)}"}

    @staticmethod
    def parse_x12(text):
        try:
            segments = text.split('~')
            parsed_data = {}
            for segment in segments:
                if segment:
                    elements = segment.split('*')
                    seg_id = elements[0]
                    if seg_id not in parsed_data:
                        parsed_data[seg_id] = []
                    parsed_data[seg_id].append(elements[1:])
            return parsed_data
        except Exception as e:
            return {"error": f"Failed to parse X12: {str(e)}"}

    @staticmethod
    def extract_and_parse_file(message_id):
        message = mongo.db.messages.find_one({"_id": ObjectId(message_id)})
        if not message:
            return

        file_path = message.get('local_path')
        initial_type = message['type']
        organization_uuid = message['organization_uuid']

        try:
            extracted_text = FileParserService.extract_text_from_file(file_path)
            
            # Store extracted text in temp collection
            temp_text_id = mongo.db.temp_extracted_texts.insert_one({
                "message_id": message_id,
                "organization_uuid": organization_uuid,
                "extracted_text": extracted_text
            }).inserted_id

            # Analyze content
            detected_type, confidence = FileParserService.analyze_content(extracted_text)

            if detected_type != initial_type:
                FileParserService._log_type_mismatch(message_id, initial_type, detected_type, confidence, organization_uuid)
                # Update stream type if this is the first file
                FileParserService._update_stream_type_if_first_file(message['stream_uuid'], detected_type, organization_uuid)

            if detected_type == 'CCDA':
                parsed_content = FileParserService.parse_ccda(extracted_text)
                parsing_status = 'completed' if 'error' not in parsed_content else 'failed'
            elif detected_type == 'X12':
                parsed_content = FileParserService.parse_x12(extracted_text)
                parsing_status = 'completed' if 'error' not in parsed_content else 'failed'
            elif detected_type == 'Clinical Notes':
                parsed_content = extracted_text
                parsing_status = 'completed'
            else:
                parsed_content = extracted_text
                parsing_status = 'completed'
                detected_type = 'RAW'

            # Update message with parsed content and status
            mongo.db.messages.update_one(
                {"_id": ObjectId(message_id)},
                {
                    "$set": {
                        "message": parsed_content,
                        "parsing_status": parsing_status,
                        "type": detected_type,
                        "parsed_at": datetime.utcnow(),
                        "conversion_status": "pending" if parsing_status == 'completed' else "failed",
                        "content_analysis": {
                            "detected_type": detected_type,
                            "confidence": confidence
                        }
                    }
                }
            )

            # Remove temporary extracted text
            mongo.db.temp_extracted_texts.delete_one({"_id": temp_text_id})

        except Exception as e:
            error_message = str(e)
            if file_path:
                error_message += f" <a href='{file_path}'>View Original File</a>"
            mongo.db.messages.update_one(
                {"_id": ObjectId(message_id)},
                {
                    "$set": {
                        "message": error_message,
                        "parsing_status": "failed",
                        "parsing_error": str(e),
                        "type": "RAW",
                        "conversion_status": "failed"
                    }
                }
            )
            FileParserService._log_error(message_id, str(e), organization_uuid)

    @staticmethod
    def _log_type_mismatch(message_id, initial_type, detected_type, confidence, organization_uuid):
        log_entry = {
            "message_id": message_id,
            "organization_uuid": organization_uuid,
            "event": "type_mismatch",
            "initial_type": initial_type,
            "detected_type": detected_type,
            "confidence": confidence,
            "timestamp": datetime.utcnow()
        }
        mongo.db.parsing_logs.insert_one(log_entry)

    @staticmethod
    def _update_stream_type_if_first_file(stream_uuid, detected_type, organization_uuid):
        stream = mongo.db.streams.find_one({"uuid": stream_uuid, "organization_uuid": organization_uuid})
        if stream and stream.get('files_processed', 0) == 0:
            mongo.db.streams.update_one(
                {"uuid": stream_uuid, "organization_uuid": organization_uuid},
                {
                    "$set": {"message_type": detected_type},
                    "$inc": {"files_processed": 1}
                }
            )
        else:
            mongo.db.streams.update_one(
                {"uuid": stream_uuid, "organization_uuid": organization_uuid},
                {"$inc": {"files_processed": 1}}
            )

    @staticmethod
    def _log_error(message_id, error_message, organization_uuid):
        log_entry = {
            "message_id": message_id,
            "organization_uuid": organization_uuid,
            "event": "parsing_error",
            "error_message": error_message,
            "timestamp": datetime.utcnow()
        }
        mongo.db.parsing_logs.insert_one(log_entry)

    @staticmethod
    def process_pending_files(organization_uuid=None):
        query = {
            "parsing_status": "pending",
            "type": {"$in": ["CCDA", "X12", "Clinical Notes"]}
        }
        if organization_uuid:
            query["organization_uuid"] = organization_uuid

        pending_messages = mongo.db.messages.find(query)
        for message in pending_messages:
            FileParserService.extract_and_parse_file(str(message['_id']))

    @staticmethod
    def get_file_type(file_path):
        _, extension = os.path.splitext(file_path)
        extension = extension.lower()
        if extension == '.pdf':
            return 'Clinical Notes'
        elif extension in ['.xml', '.ccda']:
            return 'CCDA'
        elif extension in ['.x12', '.edi']:
            return 'X12'
        else:
            return 'Unknown'

    @staticmethod
    def parse_file(file_path):
        file_type = FileParserService.get_file_type(file_path)
        if file_type == 'Clinical Notes':
            return FileParserService.extract_text_from_file(file_path)
        elif file_type == 'CCDA':
            with open(file_path, 'r') as f:
                return FileParserService.parse_ccda(f.read())
        elif file_type == 'X12':
            with open(file_path, 'r') as f:
                return FileParserService.parse_x12(f.read())
        else:
            raise ValueError(f"Unsupported file type: {file_type}")