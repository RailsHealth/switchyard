import requests
import uuid
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def send_hl7_to_fhir_converter(hl7_message):
    start_time = time.time()
    
    # Step 1: Generate UUID and Capture Timestamp
    logging.info("Step 1: Generating UUID and capturing timestamp.")
    message_uuid = str(uuid.uuid4())
    creation_timestamp = time.time()
    logging.info(f"UUID generated: {message_uuid}")
    logging.info(f"Creation Timestamp: {creation_timestamp}")

    # Step 2: Create the incoming message payload
    logging.info("Step 2: Creating incoming message payload.")
    incoming_message = {
        "UUID": message_uuid,
        "CreationTimestamp": creation_timestamp,
        "OriginalDataType": "HL7",
        "MessageBody": hl7_message
    }
    
    # Step 3: Send POST request to the API endpoint
    logging.info("Step 3: Sending POST request to the conversion API.")
    request_start_time = time.time()
    
    try:
        response = requests.post("https://fhir-conversion-api-v0.onrender.com/convert_hl7", json=incoming_message)
        response.raise_for_status()  # Raise an error for bad HTTP status codes
        request_end_time = time.time()
        logging.info(f"POST request completed in {request_end_time - request_start_time:.2f} seconds.")
        
        # Step 4: Process and log the response
        logging.info("Step 4: Processing the response.")
        outgoing_message = response.json()
        logging.info("Conversion Successful!")
        logging.info(f"UUID: {outgoing_message['UUID']}")
        logging.info(f"Creation Timestamp: {outgoing_message['CreationTimestamp']}")
        logging.info(f"Conversion Timestamp: {outgoing_message['ConversionTimestamp']}")
        logging.info(f"Original Data Type: {outgoing_message['OriginalDataType']}")
        logging.info(f"Message Body (FHIR): {outgoing_message['MessageBody']}")
        logging.info(f"Is Validated: {outgoing_message['Isvalidated']}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error during conversion: {e}")
    
    # Final: Log total time taken
    total_time = time.time() - start_time
    logging.info(f"Total time taken for the entire process: {total_time:.2f} seconds.")

# Example HL7v2 message (Replace this with your actual HL7v2 message)
hl7_message = "MSH|^~\\&|LABADT|1234|MIICE|MIICE|202308141200||ADT^A01|123456|P|2.5\rEVN|A01|202308141200\rPID|1|0001||Doe^John||19800501|M|||1234 Elm St^^Hometown^OH^12345|(123)456-7890|||M||C|123456789|987-65-4320"

# Call the function to send the HL7v2 message and print the FHIR conversion
send_hl7_to_fhir_converter(hl7_message)
