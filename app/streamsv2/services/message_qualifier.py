# app/streamsv2/services/message_qualifier.py

from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import logging
import json
from flask import current_app

from app.extensions import mongo
from app.streamsv2.models.stream_config import StreamConfig
from app.streamsv2.models.stream_status import MessageStatus, StatusManager
from app.streamsv2.utils.stream_logger import StreamLogger

logger = logging.getLogger(__name__)

class QualificationError(Exception):
    """Base exception for qualification errors"""
    pass

class MessageQualifier:
    def __init__(self, stream_config: StreamConfig):
        self.config = stream_config
        self.logger = StreamLogger(self.config.uuid)
        self.batch_size = current_app.config.get('STREAMSV2_BATCH_SIZE', 100)


    def find_qualifying_messages(self, batch_size: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            logger.info(f"Finding qualifying messages for stream {self.config.uuid}")
            
            # Get source endpoint
            source_endpoint = mongo.db.endpoints.find_one({
                "uuid": self.config.source_endpoint_uuid,
                "organization_uuid": self.config.organization_uuid
            })
            if not source_endpoint:
                logger.error(f"Source endpoint {self.config.source_endpoint_uuid} not found")
                return []

            expected_message_type = source_endpoint.get('message_type')
            if not expected_message_type:
                logger.error(f"No message type defined for endpoint {self.config.source_endpoint_uuid}")
                return []

            # Build query
            query = {
                # Must match source endpoint
                "endpoint_uuid": self.config.source_endpoint_uuid,
                "organization_uuid": self.config.organization_uuid,
                
                # Message type validation
                "type": expected_message_type,
                
                # Message status check
                "status": "received",
                
                # Stream processing status check
                "$or": [
                    # Never processed by this stream
                    {"stream_tracker": {"$exists": False}},
                    {"stream_tracker": []},
                    
                    # Not currently being processed by this stream
                    {
                        "stream_tracker": {
                            "$not": {
                                "$elemMatch": {
                                    "stream_uuid": self.config.uuid,
                                    "transformation_status": {
                                        "$in": [
                                            MessageStatus.QUALIFIED,
                                            MessageStatus.QUEUED,
                                            MessageStatus.TRANSFORMED
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    
                    # Was cancelled and can be retried
                    {
                        "stream_tracker": {
                            "$elemMatch": {
                                "stream_uuid": self.config.uuid,
                                "transformation_status": MessageStatus.CANCELLED
                            }
                        }
                    }
                ]
            }

            # Get count and batch
            total_unqualified = mongo.db.messages.count_documents(query)
            limit = batch_size or self.batch_size
            
            # Add debug logging for query
            logger.debug(f"Qualification query: {query}")
            
            messages = list(mongo.db.messages.find(query).limit(limit))
            
            logger.info(
                f"Found {total_unqualified} unqualified messages for endpoint "
                f"{self.config.source_endpoint_uuid} of type {expected_message_type}, "
                f"processing batch of {len(messages)}"
            )
            
            return messages

        except Exception as e:
            logger.error(f"Error finding qualifying messages: {str(e)}")
            return []

    def _check_source_endpoint(self) -> Optional[Dict[str, Any]]:
        """Check source endpoint exists"""
        try:
            return mongo.db.endpoints.find_one({
                "uuid": self.config.source_endpoint_uuid,
                "organization_uuid": self.config.organization_uuid,
                "deleted": {"$ne": True}
            })
        except Exception as e:
            logger.error(f"Error checking source endpoint: {str(e)}")
            return None

    def mark_messages_qualified(self, messages: List[Dict[str, Any]]) -> int:
        """Mark messages as qualified for processing"""
        qualified_count = 0
        batch_size = len(messages)
        
        logger.info(f"Starting qualification for batch of {batch_size} messages")
        
        for message in messages:
            try:
                result = mongo.db.messages.update_one(
                    {
                        "_id": message["_id"],
                        "stream_tracker.stream_uuid": {"$ne": self.config.uuid}
                    },
                    {
                        "$push": {
                            "stream_tracker": {
                                "stream_uuid": self.config.uuid,
                                "transformation_status": MessageStatus.QUALIFIED,
                                "qualified_at": datetime.utcnow(),
                                "updated_at": datetime.utcnow()
                            }
                        }
                    }
                )

                if result.modified_count > 0:
                    qualified_count += 1

            except Exception as e:
                logger.error(f"Error qualifying message {message.get('uuid')}: {str(e)}")

        logger.info(f"Batch qualification complete: {qualified_count}/{batch_size} messages qualified")
        return qualified_count

    def _qualify_message(self, message: Dict[str, Any]) -> bool:
        """Qualify a single message with transaction safety"""
        try:
            # Create qualification entry atomically
            result = mongo.db.messages.find_one_and_update(
                {
                    "_id": message["_id"],
                    # Ensure no duplicate qualification
                    "stream_tracker": {
                        "$not": {
                            "$elemMatch": {
                                "stream_uuid": self.config.uuid,
                                "transformation_status": {
                                    "$nin": [MessageStatus.ERROR, MessageStatus.CANCELLED]
                                }
                            }
                        }
                    }
                },
                {
                    "$push": {
                        "stream_tracker": {
                            "stream_uuid": self.config.uuid,
                            "transformation_status": MessageStatus.QUALIFIED,
                            "qualified_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow()
                        }
                    }
                },
                return_document=True
            )

            return bool(result)

        except Exception as e:
            logger.error(f"Error qualifying message {message.get('uuid')}: {str(e)}")
            return False

    def cancel_messages(self, status: Optional[str] = None, reason: str = "Stream stopped") -> int:
        """Cancel messages with given status"""
        try:
            logger.info(f"Cancelling messages for stream {self.config.uuid} with status {status}")
            query = {
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": self.config.uuid
                    }
                }
            }

            if status:
                query["stream_tracker"]["$elemMatch"]["transformation_status"] = status

            result = mongo.db.messages.update_many(
                query,
                {
                    "$set": {
                        "stream_tracker.$[elem].transformation_status": MessageStatus.CANCELLED,
                        "stream_tracker.$[elem].updated_at": datetime.utcnow(),
                        "stream_tracker.$[elem].cancellation_reason": reason
                    }
                },
                array_filters=[{"elem.stream_uuid": self.config.uuid}]
            )

            if result.modified_count > 0:
                logger.info(f"Cancelled {result.modified_count} messages for stream {self.config.uuid}")
                self.logger.log_event(
                    "messages_cancelled",
                    {
                        "count": result.modified_count,
                        "status": status,
                        "reason": reason
                    },
                    "info"
                )

            return result.modified_count

        except Exception as e:
            logger.error(f"Error cancelling messages: {str(e)}")
            return 0

    def get_qualification_metrics(self) -> Dict[str, Any]:
        """Get qualification metrics"""
        try:
            pipeline = [
                {
                    "$match": {
                        "stream_tracker": {
                            "$elemMatch": {
                                "stream_uuid": self.config.uuid
                            }
                        }
                    }
                },
                {
                    "$unwind": "$stream_tracker"
                },
                {
                    "$match": {
                        "stream_tracker.stream_uuid": self.config.uuid
                    }
                },
                {
                    "$group": {
                        "_id": "$stream_tracker.transformation_status",
                        "count": {"$sum": 1}
                    }
                }
            ]

            results = list(mongo.db.messages.aggregate(pipeline))
            
            metrics = {
                "qualified": 0,
                "queued": 0,
                "transformed": 0,
                "error": 0,
                "cancelled": 0
            }
            
            for result in results:
                status = result['_id'].lower()
                if status in metrics:
                    metrics[status] = result['count']

            logger.debug(f"Retrieved qualification metrics for stream {self.config.uuid}")
            return metrics

        except Exception as e:
            logger.error(f"Error getting qualification metrics for stream {self.config.uuid}: {str(e)}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }