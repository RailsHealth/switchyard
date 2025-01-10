# app/endpoints/validators/file_validator.py

import os
import logging
from typing import Any, List, Dict, Optional, Tuple, Optional
from flask import current_app

logger = logging.getLogger(__name__)

class FileValidator:
    """File validation helper for SFTP endpoints"""

    @staticmethod
    def get_allowed_extensions(message_type: str) -> List[str]:
        """
        Get allowed file extensions for a message type
        
        Args:
            message_type: Type of message (CCDA, X12, etc.)
            
        Returns:
            List of allowed extensions
        """
        try:
            extensions = current_app.config['ENDPOINT_VALIDATION']['allowed_file_extensions']
            allowed = extensions.get(message_type, [])
            logger.debug(f"Retrieved allowed extensions for {message_type}: {allowed}")
            return allowed
        except Exception as e:
            logger.error(f"Error getting allowed extensions for {message_type}: {str(e)}")
            return []

    @staticmethod
    def is_valid_extension(filename: str, message_type: str) -> bool:
        """
        Check if file extension is allowed for message type
        
        Args:
            filename: Name of file to check
            message_type: Type of message
            
        Returns:
            Boolean indicating if extension is allowed
        """
        try:
            if not filename:
                logger.warning(f"Empty filename provided for {message_type} validation")
                return False
                
            allowed_extensions = FileValidator.get_allowed_extensions(message_type)
            if not allowed_extensions:
                logger.warning(f"No allowed extensions found for message type: {message_type}")
                return False
                
            file_ext = os.path.splitext(filename)[1].lower()
            is_valid = file_ext in allowed_extensions
            
            if not is_valid:
                logger.warning(
                    f"Invalid file extension '{file_ext}' for {message_type}. "
                    f"Allowed extensions: {allowed_extensions}"
                )
            else:
                logger.debug(f"Valid file extension '{file_ext}' for {message_type}")
                
            return is_valid
            
        except Exception as e:
            logger.error(
                f"Error validating extension for file '{filename}' "
                f"(type: {message_type}): {str(e)}"
            )
            return False

    @staticmethod
    def get_extension_info(message_type: str) -> Dict[str, Any]:
        """
        Get detailed information about allowed extensions
        
        Args:
            message_type: Type of message
            
        Returns:
            Dictionary containing extension information
        """
        try:
            extensions = FileValidator.get_allowed_extensions(message_type)
            info = {
                'allowed_extensions': extensions,
                'description': f"Allowed file types for {message_type}",
                'examples': [f"example{ext}" for ext in extensions]
            }
            logger.debug(f"Retrieved extension info for {message_type}: {info}")
            return info
            
        except Exception as e:
            error_msg = f"Error getting extension info for {message_type}: {str(e)}"
            logger.error(error_msg)
            return {
                'error': error_msg,
                'allowed_extensions': [],
                'description': 'Error retrieving extension information'
            }

    @staticmethod
    def validate_file_pattern(pattern: str, message_type: str) -> Tuple[bool, Optional[str]]:
        """
        Validate that file pattern would match allowed extensions
        
        Args:
            pattern: File pattern to validate
            message_type: Type of message
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            extensions = FileValidator.get_allowed_extensions(message_type)
            if not extensions:
                error_msg = f"No file extensions configured for {message_type}"
                logger.error(error_msg)
                return False, error_msg
                
            # Check if pattern would match at least one allowed extension
            would_match = any(
                pattern.endswith(ext) or pattern.endswith(ext.upper())
                for ext in extensions
            )
            
            if not would_match:
                error_msg = (
                    f"File pattern '{pattern}' would not match any allowed extensions "
                    f"for {message_type}: {', '.join(extensions)}"
                )
                logger.warning(error_msg)
                return False, error_msg
                
            logger.debug(
                f"File pattern '{pattern}' validated successfully for {message_type}"
            )
            return True, None
            
        except Exception as e:
            error_msg = f"Error validating file pattern: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    @staticmethod
    def log_validation_metrics(filename: str, message_type: str, is_valid: bool) -> None:
        """
        Log validation metrics for monitoring
        
        Args:
            filename: Name of validated file
            message_type: Type of message
            is_valid: Whether validation passed
        """
        try:
            if is_valid:
                logger.info(
                    f"File validation passed: {filename} ({message_type})"
                )
            else:
                logger.warning(
                    f"File validation failed: {filename} ({message_type})"
                )
                
        except Exception as e:
            logger.error(f"Error logging validation metrics: {str(e)}")