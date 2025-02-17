# app/streamsv2/models/message_tracker.py

from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
from uuid import uuid4

from app.extensions import mongo
from .stream_status import MessageStatus
from app.utils.logging_utils import log_message_cycle

logger = logging.getLogger(__name__)

class MessageTracker:
    """Handles message state tracking across streams"""

    def __init__(self, stream_uuid: str):
        """Initialize message tracker"""
        self.stream_uuid = str(stream_uuid)
        self.processing_timeout = 1800  # 30 minutes

    def add_stream_tracker(self, message_uuid: str) -> bool:
        """Add stream tracker entry to message"""
        try:
            tracker_entry = {
                "stream_uuid": self.stream_uuid,
                "transformation_status": MessageStatus.QUALIFIED,
                "qualified_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "attempts": 0,
                "error": None
            }

            result = mongo.db.messages.update_one(
                {
                    "uuid": str(message_uuid),
                    "stream_tracker.stream_uuid": {"$ne": self.stream_uuid}
                },
                {
                    "$push": {
                        "stream_tracker": tracker_entry
                    }
                }
            )

            if result.modified_count > 0:
                logger.debug(f"Added stream tracker for message {message_uuid}")
                return True

            logger.warning(f"Stream tracker not added for message {message_uuid}")
            return False

        except Exception as e:
            logger.error(f"Error adding stream tracker: {str(e)}")
            return False

    def update_status(
        self, 
        message_uuid: str, 
        status: str, 
        error: Optional[str] = None
    ) -> bool:
        """Update message transformation status"""
        try:
            update_data = {
                "stream_tracker.$.transformation_status": status,
                "stream_tracker.$.updated_at": datetime.utcnow()
            }

            if error:
                update_data["stream_tracker.$.error"] = {
                    "message": error,
                    "timestamp": datetime.utcnow()
                }
                update_data["stream_tracker.$.attempts"] = 1

            result = mongo.db.messages.update_one(
                {
                    "uuid": str(message_uuid),
                    "stream_tracker.stream_uuid": self.stream_uuid
                },
                {"$set": update_data}
            )

            if result.modified_count > 0:
                log_message_cycle(
                    message_uuid=str(message_uuid),
                    event_type=f"message_{status.lower()}",
                    details={"stream_uuid": self.stream_uuid, "error": error} if error else {"stream_uuid": self.stream_uuid},
                    status="error" if error else "success"
                )
                return True

            return False

        except Exception as e:
            logger.error(f"Error updating message status: {str(e)}")
            return False

    def cancel_messages(self, status: Optional[str] = None, reason: str = "Operation cancelled") -> int:
        """Cancel messages with given status"""
        try:
            query = {
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": self.stream_uuid
                    }
                }
            }

            if status:
                query["stream_tracker"]["$elemMatch"]["transformation_status"] = status

            result = mongo.db.messages.update_many(
                query,
                {
                    "$set": {
                        "stream_tracker.$.transformation_status": MessageStatus.CANCELLED,
                        "stream_tracker.$.updated_at": datetime.utcnow(),
                        "stream_tracker.$.cancel_reason": reason
                    }
                }
            )

            if result.modified_count > 0:
                logger.info(f"Cancelled {result.modified_count} messages for stream {self.stream_uuid}")
                log_message_cycle(
                    message_uuid=str(uuid4()),
                    event_type="messages_cancelled",
                    details={
                        "stream_uuid": self.stream_uuid,
                        "count": result.modified_count,
                        "reason": reason
                    },
                    status="success"
                )

            return result.modified_count

        except Exception as e:
            logger.error(f"Error cancelling messages: {str(e)}")
            return 0

    def get_message_counts(self) -> Dict[str, int]:
        """Get message counts by status"""
        try:
            pipeline = [
                {
                    "$match": {
                        "stream_tracker": {
                            "$elemMatch": {
                                "stream_uuid": self.stream_uuid
                            }
                        }
                    }
                },
                {
                    "$unwind": "$stream_tracker"
                },
                {
                    "$match": {
                        "stream_tracker.stream_uuid": self.stream_uuid
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
            
            counts = {
                "qualified": 0,
                "queued": 0,
                "transformed": 0,
                "error": 0,
                "cancelled": 0
            }
            
            for result in results:
                status = result['_id'].lower()
                if status in counts:
                    counts[status] = result['count']

            return counts

        except Exception as e:
            logger.error(f"Error getting message counts: {str(e)}")
            return {status: 0 for status in ["qualified", "queued", "transformed", 
                                           "error", "cancelled"]}

    def get_stuck_messages(self) -> List[Dict[str, Any]]:
        """Get messages stuck in processing"""
        try:
            timeout = datetime.utcnow() - timedelta(seconds=self.processing_timeout)
            
            return list(mongo.db.messages.find({
                "stream_tracker": {
                    "$elemMatch": {
                        "stream_uuid": self.stream_uuid,
                        "transformation_status": MessageStatus.QUEUED,
                        "updated_at": {"$lt": timeout}
                    }
                }
            }))

        except Exception as e:
            logger.error(f"Error getting stuck messages: {str(e)}")
            return []

    def reset_stuck_messages(self) -> int:
        """Reset stuck messages to QUALIFIED state"""
        try:
            stuck_messages = self.get_stuck_messages()
            reset_count = 0

            for message in stuck_messages:
                if self.update_status(message['uuid'], MessageStatus.QUALIFIED):
                    reset_count += 1
                    log_message_cycle(
                        message_uuid=str(message['uuid']),
                        event_type="message_reset",
                        details={
                            "stream_uuid": self.stream_uuid,
                            "previous_status": MessageStatus.QUEUED
                        },
                        status="success"
                    )

            if reset_count > 0:
                logger.info(f"Reset {reset_count} stuck messages for stream {self.stream_uuid}")

            return reset_count

        except Exception as e:
            logger.error(f"Error resetting stuck messages: {str(e)}")
            return 0

    def cleanup_old_messages(self, days: int = 30) -> int:
        """Clean up old processed messages"""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            
            result = mongo.db.messages.update_many(
                {
                    "stream_tracker": {
                        "$elemMatch": {
                            "stream_uuid": self.stream_uuid,
                            "transformation_status": MessageStatus.TRANSFORMED,
                            "updated_at": {"$lt": cutoff}
                        }
                    }
                },
                {
                    "$pull": {
                        "stream_tracker": {
                            "stream_uuid": self.stream_uuid
                        }
                    }
                }
            )

            if result.modified_count > 0:
                logger.info(f"Cleaned up {result.modified_count} old messages for stream {self.stream_uuid}")

            return result.modified_count

        except Exception as e:
            logger.error(f"Error cleaning up old messages: {str(e)}")
            return 0