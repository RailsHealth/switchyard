from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime

@dataclass
class StreamMetrics:
    """Metrics container for streams"""
    total_messages_processed: int = 0
    total_messages_failed: int = 0
    total_bytes_transferred: int = 0
    processing_time_avg: float = 0.0
    error_rate: float = 0.0
    last_processed_at: Optional[datetime] = None
    last_error: Optional[Dict[str, Any]] = None
    processing_history: List[Dict[str, Any]] = field(default_factory=list)
    hourly_metrics: Dict[str, Any] = field(default_factory=dict)
    daily_metrics: Dict[str, Any] = field(default_factory=dict)

    def update_metrics(self, metrics_data: Dict[str, Any]) -> None:
        """Update metrics with new data"""
        self.total_messages_processed += metrics_data.get('messages_processed', 0)
        self.total_messages_failed += metrics_data.get('messages_failed', 0)
        self.total_bytes_transferred += metrics_data.get('bytes_transferred', 0)
        
        # Update processing time average
        if processing_time := metrics_data.get('processing_time'):
            if self.total_messages_processed > 0:
                self.processing_time_avg = (
                    (self.processing_time_avg * (self.total_messages_processed - 1) + processing_time) /
                    self.total_messages_processed
                )
            else:
                self.processing_time_avg = processing_time

        # Update error rate
        total_messages = self.total_messages_processed + self.total_messages_failed
        self.error_rate = (self.total_messages_failed / total_messages) if total_messages > 0 else 0.0

        # Update timestamp
        self.last_processed_at = datetime.utcnow()

        # Update last error if present
        if error := metrics_data.get('error'):
            self.last_error = {
                "message": error,
                "timestamp": datetime.utcnow()
            }

        # Add to processing history
        self.processing_history.append({
            "timestamp": datetime.utcnow(),
            "messages_processed": metrics_data.get('messages_processed', 0),
            "messages_failed": metrics_data.get('messages_failed', 0),
            "bytes_transferred": metrics_data.get('bytes_transferred', 0),
            "processing_time": metrics_data.get('processing_time')
        })

        # Keep only last 100 entries
        if len(self.processing_history) > 100:
            self.processing_history = self.processing_history[-100:]

        # Update hourly metrics
        hour_key = datetime.utcnow().strftime('%Y-%m-%d-%H')
        if hour_key not in self.hourly_metrics:
            self.hourly_metrics[hour_key] = {
                "messages_processed": 0,
                "messages_failed": 0,
                "bytes_transferred": 0,
                "errors": []
            }
        self.hourly_metrics[hour_key]["messages_processed"] += metrics_data.get('messages_processed', 0)
        self.hourly_metrics[hour_key]["messages_failed"] += metrics_data.get('messages_failed', 0)
        self.hourly_metrics[hour_key]["bytes_transferred"] += metrics_data.get('bytes_transferred', 0)
        if error:
            self.hourly_metrics[hour_key]["errors"].append(self.last_error)

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        return {
            "total_processed": self.total_messages_processed,
            "total_failed": self.total_messages_failed,
            "total_bytes": self.total_bytes_transferred,
            "avg_processing_time": self.processing_time_avg,
            "error_rate": self.error_rate,
            "last_processed": self.last_processed_at,
            "last_error": self.last_error
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        return {
            "total_messages_processed": self.total_messages_processed,
            "total_messages_failed": self.total_messages_failed,
            "total_bytes_transferred": self.total_bytes_transferred,
            "processing_time_avg": self.processing_time_avg,
            "error_rate": self.error_rate,
            "last_processed_at": self.last_processed_at,
            "last_error": self.last_error,
            "processing_history": self.processing_history,
            "hourly_metrics": self.hourly_metrics,
            "daily_metrics": self.daily_metrics
        }