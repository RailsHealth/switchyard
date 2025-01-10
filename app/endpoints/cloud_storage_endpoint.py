# app/endpoints/cloud_storage_endpoint.py

from __future__ import annotations
import threading
import time
from typing import Optional, Dict, Any, Union, Tuple, List
from datetime import datetime, timedelta
import uuid
from abc import ABC, abstractmethod
import json
from flask import current_app
import os
from dataclasses import dataclass, field

from app.endpoints.base import BaseEndpoint
from app.endpoints.endpoint_types import EndpointConfig, EndpointStatus
from app.utils.logging_utils import log_message_cycle

# AWS imports
import boto3
from botocore.exceptions import ClientError
from botocore.client import Config as BotoConfig

# GCP imports
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
from google.oauth2 import service_account

class CloudStorageError(Exception):
    """Base exception for cloud storage operations"""
    pass

@dataclass
class StorageMetrics:
    """Storage provider metrics container"""
    provider: str
    bucket_name: str
    files_uploaded: int = 0
    bytes_transferred: int = 0 
    upload_errors: int = 0
    connection_errors: int = 0
    last_upload_time: Optional[datetime] = None
    encryption_enabled: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

class CloudStorageProvider(ABC):
    """Base class for cloud storage providers"""
    
    def __init__(self):
        """Initialize provider"""
        self.metrics = StorageMetrics(
            provider=self.__class__.__name__,
            bucket_name=""
        )
        self._lock = threading.Lock()
        
    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize cloud storage client"""
        pass
        
    @abstractmethod
    def upload_file(self, local_path: str, remote_path: str, 
                   encryption_config: Optional[Dict] = None) -> str:
        """Upload file to cloud storage"""
        pass
        
    @abstractmethod
    def check_connection(self) -> bool:
        """Test connection to cloud storage"""
        pass
        
    @abstractmethod
    def get_storage_metrics(self) -> Dict[str, Any]:
        """Get provider-specific metrics"""
        pass

    @abstractmethod
    def validate_encryption_config(self, encryption_config: Dict) -> bool:
        """Validate encryption configuration"""
        pass

    def update_metrics(self, update_func) -> None:
        """Thread-safe metrics update"""
        with self._lock:
            update_func(self.metrics)

class AWSProvider(CloudStorageProvider):
    """AWS S3 storage provider implementation"""
    
    ENCRYPTION_TYPES = {
        'none': 'No encryption',
        'AES256': 'SSE-S3 (AES-256)',
        'aws:kms': 'AWS KMS',
        'customer-provided': 'Customer-Provided Keys'
    }
    
    STORAGE_CLASSES = [
        'STANDARD',
        'STANDARD_IA',
        'ONEZONE_IA',
        'INTELLIGENT_TIERING',
        'GLACIER',
        'DEEP_ARCHIVE'
    ]
    
    def __init__(self):
        """Initialize AWS provider"""
        super().__init__()
        self.client = None
        self.bucket_name = None
        self.region = None
        self.encryption_config = None
        self.kms_key_id = None
        self._default_storage_class = 'STANDARD'

    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize S3 client with configuration"""
        try:
            # Validate and extract configuration
            self.bucket_name = config.get('bucket')
            if not self.bucket_name:
                raise CloudStorageError("S3 bucket name is required")
                
            self.region = config.get('region', 'us-east-1')
            self.encryption_config = config.get('encryption', {})
            self.kms_key_id = config.get('kms_key_id')
            
            # Validate credentials
            if not all(k in config for k in ['aws_access_key_id', 'aws_secret_access_key']):
                raise CloudStorageError("AWS credentials are required")
            
            # Configure client
            client_config = BotoConfig(
                region_name=self.region,
                signature_version='s3v4',
                retries={
                    'max_attempts': 3,
                    'mode': 'standard'
                }
            )
            
            # Create session and client
            session = boto3.Session(
                aws_access_key_id=config['aws_access_key_id'],
                aws_secret_access_key=config['aws_secret_access_key'],
                region_name=self.region
            )
            
            self.client = session.client('s3', config=client_config)
            
            # Validate bucket access
            self.client.head_bucket(Bucket=self.bucket_name)
            
            # Update metrics
            self.metrics.bucket_name = self.bucket_name
            self.metrics.metadata.update({
                'region': self.region,
                'encryption_type': self.encryption_config.get('type', 'none'),
                'storage_class': self._default_storage_class
            })
            
            # Check bucket encryption
            try:
                encryption = self.client.get_bucket_encryption(Bucket=self.bucket_name)
                current_app.logger.info(
                    f"Bucket {self.bucket_name} has default encryption: {encryption}"
                )
                self.metrics.encryption_enabled = True
            except ClientError as e:
                if e.response['Error']['Code'] == 'ServerSideEncryptionConfigurationNotFoundError':
                    current_app.logger.info(
                        f"Bucket {self.bucket_name} does not have default encryption"
                    )
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                raise CloudStorageError(f"Bucket {self.bucket_name} does not exist")
            elif error_code in ['AccessDenied', 'InvalidAccessKeyId', 'SignatureDoesNotMatch']:
                raise CloudStorageError("Invalid AWS credentials or insufficient permissions")
            else:
                raise CloudStorageError(f"AWS error: {str(e)}")
        except Exception as e:
            raise CloudStorageError(f"Failed to initialize AWS S3 client: {str(e)}")

    def upload_file(self, local_path: str, remote_path: str, 
                   encryption_config: Optional[Dict] = None) -> str:
        """Upload file to S3 with encryption and metadata"""
        if not self.client:
            raise CloudStorageError("S3 client not initialized")
            
        try:
            extra_args = {}
            
            # Handle encryption configuration
            enc_config = encryption_config or self.encryption_config
            if enc_config:
                enc_type = enc_config.get('type')
                if enc_type == 'AES256':
                    extra_args['ServerSideEncryption'] = 'AES256'
                elif enc_type == 'aws:kms':
                    extra_args['ServerSideEncryption'] = 'aws:kms'
                    if self.kms_key_id:
                        extra_args['SSEKMSKeyId'] = self.kms_key_id
                elif enc_type == 'customer-provided':
                    if 'customer_key' in enc_config:
                        extra_args['SSECustomerAlgorithm'] = 'AES256'
                        extra_args['SSECustomerKey'] = enc_config['customer_key']
            
            # Add metadata if provided
            if metadata := encryption_config.get('metadata'):
                extra_args['Metadata'] = metadata
            
            # Set storage class
            storage_class = encryption_config.get('storage_class', self._default_storage_class)
            if storage_class in self.STORAGE_CLASSES:
                extra_args['StorageClass'] = storage_class
            
            # Upload file
            self.client.upload_file(
                local_path,
                self.bucket_name,
                remote_path,
                ExtraArgs=extra_args
            )
            
            # Update metrics
            file_size = os.path.getsize(local_path)
            self.update_metrics(lambda m: setattr(m, 'files_uploaded', m.files_uploaded + 1))
            self.update_metrics(lambda m: setattr(m, 'bytes_transferred', 
                                                m.bytes_transferred + file_size))
            self.update_metrics(lambda m: setattr(m, 'last_upload_time', datetime.utcnow()))
            
            # Log encryption status
            if not enc_config or enc_config.get('type') == 'none':
                current_app.logger.info(f"File {remote_path} uploaded without encryption")
            else:
                current_app.logger.info(
                    f"File {remote_path} uploaded with {enc_config['type']} encryption"
                )
            
            return f"s3://{self.bucket_name}/{remote_path}"
            
        except Exception as e:
            self.update_metrics(lambda m: setattr(m, 'upload_errors', m.upload_errors + 1))
            raise CloudStorageError(f"S3 upload failed: {str(e)}")

    def check_connection(self) -> bool:
        """Test S3 connection and basic operations"""
        if not self.client:
            return False
            
        try:
            # Test bucket access
            self.client.head_bucket(Bucket=self.bucket_name)
            
            # Test operations with temporary file
            test_key = f"test/connection_test_{uuid.uuid4()}"
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=test_key,
                Body=b"connection test"
            )
            self.client.delete_object(
                Bucket=self.bucket_name,
                Key=test_key
            )
            
            return True
            
        except Exception as e:
            current_app.logger.error(f"S3 connection test failed: {str(e)}")
            self.update_metrics(lambda m: setattr(m, 'connection_errors', 
                                                m.connection_errors + 1))
            return False

    def get_storage_metrics(self) -> Dict[str, Any]:
        """Get comprehensive S3 metrics and configuration"""
        if not self.client:
            return {
                "error": "S3 client not initialized",
                "provider": "aws_s3"
            }
        
        metrics = {
            "provider": "aws_s3",
            "bucket": self.bucket_name,
            "region": self.region,
            "encryption_enabled": self.metrics.encryption_enabled,
            "files_uploaded": self.metrics.files_uploaded,
            "bytes_transferred": self.metrics.bytes_transferred,
            "upload_errors": self.metrics.upload_errors,
            "connection_errors": self.metrics.connection_errors
        }
        
        try:
            # Get bucket versioning
            versioning = self.client.get_bucket_versioning(Bucket=self.bucket_name)
            metrics["versioning_enabled"] = versioning.get('Status') == 'Enabled'
            
            # Get encryption configuration
            try:
                encryption = self.client.get_bucket_encryption(Bucket=self.bucket_name)
                metrics["default_encryption"] = True
                metrics["encryption_type"] = encryption.get(
                    'ServerSideEncryptionConfiguration', {}
                )
            except ClientError:
                metrics["default_encryption"] = False
            
            # Get bucket size
            size_metrics = self.client.get_metric_statistics(
                Namespace='AWS/S3',
                MetricName='BucketSizeBytes',
                Dimensions=[{'Name': 'BucketName', 'Value': self.bucket_name}],
                StartTime=datetime.utcnow() - timedelta(days=1),
                EndTime=datetime.utcnow(),
                Period=86400,
                Statistics=['Average']
            )
            if size_metrics.get('Datapoints'):
                metrics["approximate_size"] = size_metrics['Datapoints'][0]['Average']
            
            # Get lifecycle rules
            try:
                lifecycle = self.client.get_bucket_lifecycle_configuration(
                    Bucket=self.bucket_name
                )
                metrics["lifecycle_rules"] = len(lifecycle.get('Rules', []))
            except ClientError:
                metrics["lifecycle_rules"] = 0
            
            # Get logging configuration
            try:
                logging = self.client.get_bucket_logging(Bucket=self.bucket_name)
                metrics["logging_enabled"] = 'LoggingEnabled' in logging
            except ClientError:
                metrics["logging_enabled"] = False
            
        except Exception as e:
            current_app.logger.error(f"Error fetching S3 metrics: {str(e)}")
            
        return metrics

    def validate_encryption_config(self, encryption_config: Dict) -> bool:
        """Validate S3 encryption configuration"""
        if not encryption_config:
            return True  # No encryption is valid
            
        enc_type = encryption_config.get('type')
        if enc_type not in self.ENCRYPTION_TYPES:
            return False
            
        # Validate KMS configuration
        if enc_type == 'aws:kms' and not self.kms_key_id:
            return False
            
        # Validate customer-provided keys
        if enc_type == 'customer-provided' and 'customer_key' not in encryption_config:
            return False
            
        return True

class GCPProvider(CloudStorageProvider):
    """GCP Storage provider implementation"""
    
    ENCRYPTION_TYPES = {
        'none': 'No encryption',
        'google-managed': 'Google-managed encryption',
        'customer-managed': 'Customer-managed encryption (CMEK)',
        'customer-supplied': 'Customer-supplied encryption keys (CSEK)'
    }
    
    STORAGE_CLASSES = [
        'STANDARD',
        'NEARLINE',
        'COLDLINE',
        'ARCHIVE'
    ]
    
    def __init__(self):
        """Initialize GCP provider"""
        super().__init__()
        self.client = None
        self.bucket = None
        self.project_id = None
        self.encryption_config = None
        self.kms_key_name = None
        self._default_storage_class = 'STANDARD'

    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize GCS client with configuration"""
        try:
            self.project_id = config.get('project_id')
            if not self.project_id:
                raise CloudStorageError("GCP project ID is required")
                
            bucket_name = config.get('bucket')
            if not bucket_name:
                raise CloudStorageError("GCS bucket name is required")
                
            self.encryption_config = config.get('encryption', {})
            self.kms_key_name = config.get('kms_key_name')
            
            # Initialize client with credentials
            if 'credentials_json' in config:
                try:
                    credentials = service_account.Credentials.from_service_account_info(
                        json.loads(config['credentials_json'])
                    )
                    self.client = storage.Client(
                        credentials=credentials,
                        project=self.project_id
                    )
                except Exception as e:
                    raise CloudStorageError(f"Invalid GCP credentials: {str(e)}")
            else:
                self.client = storage.Client(project=self.project_id)
            
            # Get and validate bucket
            self.bucket = self.client.bucket(bucket_name)
            if not self.bucket.exists():
                raise CloudStorageError(f"Bucket {bucket_name} does not exist")
            
            # Configure default KMS key if provided
            if self.kms_key_name:
                self.bucket.default_kms_key_name = self.kms_key_name
                self.bucket.patch()
            
            # Update metrics
            self.metrics.bucket_name = bucket_name
            self.metrics.metadata.update({
                'project_id': self.project_id,
                'location': self.bucket.location,
                'storage_class': self._default_storage_class,
                'encryption_type': (
                    'customer-managed' if self.bucket.default_kms_key_name 
                    else 'google-managed'
                )
            })
            
            # Log encryption status
            current_app.logger.info(
                f"Bucket {bucket_name} using "
                f"{'Customer-managed' if self.bucket.default_kms_key_name else 'Google-managed'} "
                "encryption"
            )
            
        except Exception as e:
            raise CloudStorageError(f"Failed to initialize GCP Storage client: {str(e)}")

    def upload_file(self, local_path: str, remote_path: str, 
                   encryption_config: Optional[Dict] = None) -> str:
        """Upload file to GCS with encryption and metadata"""
        if not self.client or not self.bucket:
            raise CloudStorageError("GCS client not initialized")
            
        try:
            blob = self.bucket.blob(remote_path)
            
            # Handle encryption
            enc_config = encryption_config or self.encryption_config
            if enc_config:
                enc_type = enc_config.get('type')
                if enc_type == 'customer-managed':
                    blob.kms_key_name = self.kms_key_name
                elif enc_type == 'customer-supplied':
                    if 'encryption_key' in enc_config:
                        blob.encryption_key = enc_config['encryption_key']
            
            # Set storage class if specified
            storage_class = encryption_config.get('storage_class', self._default_storage_class)
            if storage_class in self.STORAGE_CLASSES:
                blob.storage_class = storage_class
            
            # Add metadata if provided
            if metadata := encryption_config.get('metadata'):
                blob.metadata = metadata
            
            # Upload file
            blob.upload_from_filename(local_path)
            
            # Update metrics
            file_size = os.path.getsize(local_path)
            self.update_metrics(lambda m: setattr(m, 'files_uploaded', m.files_uploaded + 1))
            self.update_metrics(lambda m: setattr(m, 'bytes_transferred', 
                                                m.bytes_transferred + file_size))
            self.update_metrics(lambda m: setattr(m, 'last_upload_time', datetime.utcnow()))
            
            # Log encryption status
            if not enc_config or enc_config.get('type') == 'none':
                current_app.logger.info(f"File {remote_path} uploaded with default encryption")
            else:
                current_app.logger.info(
                    f"File {remote_path} uploaded with {enc_type} encryption"
                )
            
            return f"gs://{self.bucket.name}/{remote_path}"
            
        except Exception as e:
            self.update_metrics(lambda m: setattr(m, 'upload_errors', m.upload_errors + 1))
            raise CloudStorageError(f"GCS upload failed: {str(e)}")

    def check_connection(self) -> bool:
        """Test GCS connection and basic operations"""
        if not self.client or not self.bucket:
            return False
            
        try:
            # Test bucket existence
            if not self.bucket.exists():
                return False
                
            # Test basic operations
            test_blob = self.bucket.blob(f"test/connection_test_{uuid.uuid4()}")
            test_blob.upload_from_string("connection test")
            test_blob.delete()
            
            return True
            
        except Exception as e:
            current_app.logger.error(f"GCS connection test failed: {str(e)}")
            self.update_metrics(lambda m: setattr(m, 'connection_errors', 
                                                m.connection_errors + 1))
            return False

    def get_storage_metrics(self) -> Dict[str, Any]:
        """Get comprehensive GCS metrics and configuration"""
        if not self.client or not self.bucket:
            return {
                "error": "GCS client not initialized",
                "provider": "gcp_storage"
            }
        
        metrics = {
            "provider": "gcp_storage",
            "bucket": self.bucket.name,
            "project_id": self.project_id,
            "encryption_enabled": bool(self.bucket.default_kms_key_name),
            "files_uploaded": self.metrics.files_uploaded,
            "bytes_transferred": self.metrics.bytes_transferred,
            "upload_errors": self.metrics.upload_errors,
            "connection_errors": self.metrics.connection_errors
        }
        
        try:
            metrics.update({
                "location": self.bucket.location,
                "storage_class": self.bucket.storage_class,
                "versioning_enabled": self.bucket.versioning_enabled,
                "lifecycle_rules": len(self.bucket.lifecycle_rules or []),
                "default_encryption": (
                    "customer-managed" if self.bucket.default_kms_key_name 
                    else "google-managed"
                ),
                "public_access_prevention": self.bucket.iam_configuration.public_access_prevention,
                "uniform_access": self.bucket.iam_configuration.uniform_bucket_level_access_enabled,
                "requester_pays": self.bucket.requester_pays,
                "retention_period": self.bucket.retention_period,
                "default_event_based_hold": self.bucket.default_event_based_hold
            })
            
            # Get bucket statistics
            try:
                iterator = self.bucket.list_blobs(max_results=1000)
                blobs = list(iterator)
                metrics.update({
                    "object_count": len(blobs),
                    "total_bytes": sum(b.size for b in blobs)
                })
            except Exception as e:
                current_app.logger.warning(f"Error fetching bucket statistics: {str(e)}")
            
        except Exception as e:
            current_app.logger.error(f"Error fetching GCS metrics: {str(e)}")
            
        return metrics

    def validate_encryption_config(self, encryption_config: Dict) -> bool:
        """Validate GCS encryption configuration"""
        if not encryption_config:
            return True  # No encryption is valid
            
        enc_type = encryption_config.get('type')
        if enc_type not in self.ENCRYPTION_TYPES:
            return False
            
        # Validate CMEK configuration
        if enc_type == 'customer-managed' and not self.kms_key_name:
            return False
            
        # Validate CSEK configuration
        if enc_type == 'customer-supplied' and 'encryption_key' not in encryption_config:
            return False
            
        return True

class CloudStorageEndpoint(BaseEndpoint):
    """
    Cloud Storage endpoint implementation
    
    Supports:
    - AWS S3
    - Google Cloud Storage
    - Multiple encryption options
    - Various storage configurations
    """

    # Supported storage providers
    SUPPORTED_PROVIDERS = {
        'aws_s3': AWSProvider,
        'gcp_storage': GCPProvider
    }

    def __init__(self, config: EndpointConfig, **kwargs):
        """Initialize cloud storage endpoint"""
        super().__init__(config=config, **kwargs)
        
        # Validate provider
        provider = kwargs.get('provider')
        if provider not in self.SUPPORTED_PROVIDERS:
            raise ValueError(f"Unsupported storage provider: {provider}")
            
        # Initialize provider
        self.provider = provider
        self.storage_config = kwargs.get('storage_config', {})
        self.storage_client = self.SUPPORTED_PROVIDERS[provider]()
        
        # File operation tracking
        self.upload_queue = []
        self.failed_uploads = []
        self.upload_lock = threading.Lock()
        
        # Initialize metrics
        self.metrics.update({
            'provider': provider,
            'files_uploaded': 0,
            'bytes_uploaded': 0,
            'upload_errors': 0,
            'connection_errors': 0,
            'last_upload_time': None
        })

    def start(self) -> bool:
        """Start cloud storage endpoint"""
        if not super().start():
            return False

        try:
            # Initialize storage client
            self.storage_client.initialize(self.storage_config)
            
            # Test connection
            if not self.storage_client.check_connection():
                raise CloudStorageError("Failed to connect to cloud storage")

            self.log_info(f"Started {self.provider} endpoint successfully")
            
            # Log successful start
            self.log_message_cycle(
                message_uuid=str(uuid.uuid4()),
                event_type="endpoint_started",
                details={
                    "provider": self.provider,
                    "storage_config": {
                        k: v for k, v in self.storage_config.items() 
                        if k not in ['aws_secret_access_key', 'credentials_json']
                    }
                },
                status="success"
            )
            
            return True

        except Exception as e:
            self.log_error(f"Failed to start cloud storage endpoint: {str(e)}")
            self._set_error_status()
            return False

    def stop(self) -> bool:
        """Stop endpoint operations"""
        try:
            status = super().stop()
            
            # Clear queues and counters
            with self.upload_lock:
                self.upload_queue.clear()
                self.failed_uploads.clear()
            
            self.log_info(f"Stopped {self.provider} endpoint")
            
            # Log stop event
            self.log_message_cycle(
                message_uuid=str(uuid.uuid4()),
                event_type="endpoint_stopped",
                details={"provider": self.provider},
                status="success"
            )
            
            return status
            
        except Exception as e:
            self.log_error(f"Error stopping endpoint: {str(e)}")
            return False

    def upload_file(self, local_path: str, remote_path: str, 
                   encryption_config: Optional[Dict] = None,
                   metadata: Optional[Dict] = None) -> Optional[str]:
        """Upload file to cloud storage"""
        if not os.path.exists(local_path):
            self.log_error(f"Local file not found: {local_path}")
            return None
            
        try:
            message_uuid = str(uuid.uuid4())
            
            # Add metadata if provided
            if metadata:
                encryption_config = encryption_config or {}
                encryption_config['metadata'] = metadata
            
            # Log upload start
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="upload_started",
                details={
                    "local_path": local_path,
                    "remote_path": remote_path,
                    "file_size": os.path.getsize(local_path)
                },
                status="started"
            )
            
            # Upload file
            cloud_url = self.storage_client.upload_file(
                local_path, 
                remote_path,
                encryption_config
            )
            
            # Update metrics
            self.metrics['files_uploaded'] += 1
            self.metrics['bytes_uploaded'] += os.path.getsize(local_path)
            self.metrics['last_upload_time'] = datetime.utcnow()
            
            # Log success
            self.log_info(f"File uploaded successfully: {cloud_url}")
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="upload_completed",
                details={"cloud_url": cloud_url},
                status="success"
            )
            
            return cloud_url
            
        except Exception as e:
            self.metrics['upload_errors'] += 1
            self.log_error(f"File upload failed: {str(e)}")
            
            # Track failed upload
            with self.upload_lock:
                self.failed_uploads.append({
                    "local_path": local_path,
                    "remote_path": remote_path,
                    "error": str(e),
                    "timestamp": datetime.utcnow()
                })
            
            # Log failure
            self.log_message_cycle(
                message_uuid=message_uuid,
                event_type="upload_failed",
                details={"error": str(e)},
                status="error"
            )
            
            return None

    def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive metrics"""
        metrics = super().get_metrics()
        
        # Get cloud storage specific metrics
        try:
            storage_metrics = self.storage_client.get_storage_metrics()
            
            metrics.update({
                "provider": self.provider,
                "files_uploaded": self.metrics.get('files_uploaded', 0),
                "bytes_uploaded": self.metrics.get('bytes_uploaded', 0),
                "upload_errors": self.metrics.get('upload_errors', 0),
                "connection_errors": self.metrics.get('connection_errors', 0),
                "last_upload_time": (
                    self.metrics.get('last_upload_time').isoformat() 
                    if self.metrics.get('last_upload_time') 
                    else None
                ),
                "failed_uploads": len(self.failed_uploads),
                "storage_metrics": storage_metrics
            })
        except Exception as e:
            self.log_error(f"Error fetching storage metrics: {str(e)}")
            metrics["storage_metrics"] = {"error": "Unable to fetch storage metrics"}
        
        return metrics

    def cleanup(self) -> None:
        """Perform cleanup operations"""
        try:
            self.stop()
            
            # Clear tracking data
            with self.upload_lock:
                self.upload_queue.clear()
                self.failed_uploads.clear()
            
            self.log_info("Cleanup completed successfully")
            
        except Exception as e:
            self.log_error(f"Error during cleanup: {str(e)}")

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection to cloud storage"""
        try:
            if self.storage_client.check_connection():
                return True, "Connection successful"
            return False, "Failed to connect to cloud storage"
        except Exception as e:
            return False, f"Error testing connection: {str(e)}"

    def get_encryption_status(self) -> Dict[str, Any]:
        """Get current encryption status"""
        try:
            if self.provider == 'aws_s3':
                return self._get_s3_encryption_status()
            else:  # gcp_storage
                return self._get_gcs_encryption_status()
        except Exception as e:
            self.log_error(f"Error getting encryption status: {str(e)}")
            return {"status": "error", "message": str(e)}

    def _get_s3_encryption_status(self) -> Dict[str, Any]:
        """Get S3 encryption status"""
        client = self.storage_client.client
        bucket = self.storage_client.bucket_name
        
        status = {
            "provider": "aws_s3",
            "encryption_type": self.storage_config.get('encryption', {}).get('type', 'none'),
            "bucket_encryption": None
        }
        
        try:
            encryption = client.get_bucket_encryption(Bucket=bucket)
            status["bucket_encryption"] = encryption['ServerSideEncryptionConfiguration']
            status["kms_key_id"] = self.storage_client.kms_key_id
        except ClientError as e:
            if e.response['Error']['Code'] == 'ServerSideEncryptionConfigurationNotFoundError':
                status["bucket_encryption"] = "Not configured"
        
        return status

    def _get_gcs_encryption_status(self) -> Dict[str, Any]:
        """Get GCS encryption status"""
        bucket = self.storage_client.bucket
        return {
            "provider": "gcp_storage",
            "encryption_type": self.storage_config.get('encryption', {}).get('type', 'google-managed'),
            "default_kms_key": bucket.default_kms_key_name or "Google-managed",
            "uniform_access": bucket.iam_configuration.uniform_bucket_level_access_enabled,
            "kms_key_name": self.storage_client.kms_key_name
        }

    def get_failed_uploads(self) -> List[Dict[str, Any]]:
        """Get list of failed uploads"""
        with self.upload_lock:
            return self.failed_uploads.copy()

    def clear_failed_uploads(self) -> None:
        """Clear failed uploads list"""
        with self.upload_lock:
            self.failed_uploads.clear()

    def retry_failed_uploads(self) -> Tuple[int, int]:
        """
        Retry failed uploads
        
        Returns:
            Tuple of (successful retries, remaining failures)
        """
        successful = 0
        remaining = 0
        
        with self.upload_lock:
            retries = self.failed_uploads.copy()
            self.failed_uploads.clear()
            
        for upload in retries:
            try:
                result = self.upload_file(
                    upload['local_path'],
                    upload['remote_path']
                )
                
                if result:
                    successful += 1
                else:
                    remaining += 1
                    with self.upload_lock:
                        self.failed_uploads.append(upload)
                        
            except Exception as e:
                self.log_error(f"Retry failed for {upload['local_path']}: {str(e)}")
                remaining += 1
                with self.upload_lock:
                    self.failed_uploads.append(upload)
        
        return successful, remaining

    def validate_storage_class(self, storage_class: str) -> bool:
        """
        Validate storage class for current provider
        
        Args:
            storage_class: Storage class to validate
            
        Returns:
            Boolean indicating if storage class is valid
        """
        if self.provider == 'aws_s3':
            return storage_class in AWSProvider.STORAGE_CLASSES
        else:  # gcp_storage
            return storage_class in GCPProvider.STORAGE_CLASSES

    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> bool:
        """Validate cloud storage endpoint configuration"""
        try:
            # Validate base configuration
            endpoint_config = EndpointConfig.from_dict(config)
            is_valid, error = endpoint_config.validate()
            if not is_valid:
                return False
            
            # Validate provider
            provider = config.get('provider')
            if provider not in cls.SUPPORTED_PROVIDERS:
                return False
                
            # Validate storage configuration
            storage_config = config.get('storage_config', {})
            if not storage_config:
                return False
                
            # Create provider instance for validation
            provider_instance = cls.SUPPORTED_PROVIDERS[provider]()
            
            # Validate encryption if configured
            encryption_config = storage_config.get('encryption')
            if encryption_config:
                if not provider_instance.validate_encryption_config(encryption_config):
                    return False
            
            return True
            
        except Exception as e:
            current_app.logger.error(f"Cloud storage config validation error: {str(e)}")
            return False

    def __str__(self) -> str:
        """String representation"""
        return (f"CloudStorageEndpoint(uuid={self.config.uuid}, name={self.config.name}, "
                f"provider={self.provider})")

    def __repr__(self) -> str:
        """Detailed string representation"""
        return (f"CloudStorageEndpoint(uuid={self.config.uuid}, name={self.config.name}, "
                f"mode={self.config.mode}, message_type={self.config.message_type}, "
                f"endpoint_type={self.config.endpoint_type}, "
                f"organization_uuid={self.config.organization_uuid}, "
                f"provider={self.provider}, active={self.config.active})")