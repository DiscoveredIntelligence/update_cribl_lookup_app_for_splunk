#!/usr/bin/env python3
"""
Cribl Sync Engine - Core API wrapper for Cribl Cloud operations
"""

import csv
import io
import logging
import random
import requests
import time

from typing import (
    Dict, 
    List, 
    Optional, 
    Tuple
)

class CriblSyncEngine:
    
    
    def __init__(self, cribl_url: str, client_id: str, client_secret: str, session_key: str = None, 
                 logger=None, verify_ssl: bool = True, max_retries: int = 3, retry_delay_base: int = 2,
                 deploy_max_wait_time: int = 600, deploy_retry_interval: int = 30, http_debug_level: str = "summary",
                 skip_mode_detection: bool = False):
        """Initialize the Cribl Sync Engine.
        
        Args:
            cribl_url: Full Cribl Cloud URL (e.g., https://main-org-id.cribl.cloud)
            client_id: OAuth Client ID
            client_secret: OAuth Client Secret
            session_key: Splunk session key (for logging context)
            logger: Optional logger instance
            verify_ssl: Whether to verify SSL certificates
            max_retries: Maximum number of retry attempts for operations
            retry_delay_base: Base delay in seconds for exponential backoff
            deploy_max_wait_time: Maximum time in seconds to wait for deployment (default: 600 = 10 minutes)
            deploy_retry_interval: Base interval in seconds for deployment retry backoff (default: 30)
            http_debug_level: Level of HTTP debug logging: "off", "basic", "summary", "full" (default: "summary")
                             - "off": No HTTP debug logging
                             - "basic": Only status codes and URLs
                             - "summary": Status, URLs, and summarized responses for large data
                             - "full": Complete request/response data (legacy behavior)
            skip_mode_detection: Skip leader/single mode detection and use default timeout (default: False)
        """
        self.cribl_cloud_url = cribl_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.session_key = session_key
        self.logger = logger or logging.getLogger('updatecribllookup')
        self.verify_ssl = verify_ssl
        self.token = None
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base
        self.deploy_max_wait_time = deploy_max_wait_time
        self.deploy_retry_interval = deploy_retry_interval
        self.http_debug_level = http_debug_level.lower()
        self.skip_mode_detection = skip_mode_detection
        self.current_search_id = None  # Track current search ID for logging context
    

    def _get_search_id_prefix(self):
        """Get search ID prefix for log messages."""
        return f"({self.current_search_id}) " if hasattr(self, 'current_search_id') and self.current_search_id else ""
        
        
    def _retry_with_backoff(self, operation_name: str, operation_func, *args, progress_callback=None, **kwargs):
        """Retry failed operations with exponential backoff to handle transient API issues."""
        last_exception = None
        search_id_prefix = self._get_search_id_prefix()
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.debug(f"{search_id_prefix}{operation_name}: Attempt {attempt}/{self.max_retries}")
                
                # Send progress update
                if progress_callback:
                    progress_callback(f"{operation_name}: Attempt {attempt}/{self.max_retries}")
                
                result = operation_func(*args, **kwargs)
                
                if attempt > 1:
                    self.logger.info(f"{search_id_prefix}{operation_name}: Succeeded on attempt {attempt}")
                
                return result
                
            except Exception as e:
                last_exception = e
                self.logger.info(f"{search_id_prefix}{operation_name}: Attempt {attempt} failed: {str(e)}")
                
                if attempt < self.max_retries:
                    # Calculate delay with exponential backoff 
                    delay = self.retry_delay_base ** attempt + random.uniform(0, 1)
                    self.logger.info(f"{search_id_prefix}{operation_name}: Retrying in {delay:.1f} seconds...")
                    
                    # Send progress update during delay
                    if progress_callback:
                        msg = f"{operation_name}: Retrying in {delay:.1f} seconds (attempt {attempt + 1}/" \
                              f"{self.max_retries})"
                        progress_callback(msg)
                    
                    time.sleep(delay)
                else:
                    self.logger.error(f"{search_id_prefix}{operation_name}: All {self.max_retries} attempts failed")
        
        raise last_exception


    def _retry_with_backoff_generator(self, operation_name: str, operation_func, *args, **kwargs):
        """Generator-based retry with exponential backoff that yields progress."""
        last_exception = None
        search_id_prefix = self._get_search_id_prefix()
        
        for attempt in range(1, self.max_retries + 1):
            try:
                yield {'type': 'progress', 'message': f"{operation_name}: Attempt {attempt}/{self.max_retries}"}
                
                result = operation_func(*args, **kwargs)
                yield {'type': 'success', 'result': result}
                return
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"{search_id_prefix}{operation_name}: Attempt {attempt} failed: {str(e)}")
                
                if attempt < self.max_retries:
                    delay = self.retry_delay_base ** attempt + random.uniform(0, 1)
                    msg = f"{operation_name}: Retrying in {delay:.1f} seconds (attempt {attempt + 1}/" \
                          f"{self.max_retries})"
                    yield {'type': 'progress', 'message': msg}
                    time.sleep(delay)
                else:
                    self.logger.error(f"{search_id_prefix}{operation_name}: All {self.max_retries} attempts failed")
        
                    err_msg = f"{operation_name}: All {self.max_retries} attempts failed: {str(last_exception)}"
                    yield {'type': 'error', 'message': err_msg}
    

    def _debug_http_request(self, method: str, url: str, headers: dict = None,
                            data: dict = None, 
                            json_data: dict = None, response=None):
        """HTTP request/response debugging."""
        
        # Skip all HTTP debugging if disabled
        if self.http_debug_level == "off":
            return
            
        search_id_prefix = self._get_search_id_prefix()
        
        # Basic level: only log essential info
        if self.http_debug_level == "basic":
            self.logger.debug(f"{search_id_prefix}HTTP {method.upper()}: {url}")
            if response:
                self.logger.debug(f"{search_id_prefix}HTTP Response: {response.status_code}")
            return
        
        # Summary and Full levels: more detailed logging
        self.logger.debug(f"{search_id_prefix}=== HTTP {method.upper()} REQUEST DEBUG ===")
        self.logger.debug(f"{search_id_prefix}URL: {url}")
        
        if headers:
            # Sanitize sensitive headers
            debug_headers = dict(headers)
            if 'Authorization' in debug_headers:
                auth_header = debug_headers['Authorization']
                if auth_header.startswith('Bearer '):
                    debug_headers['Authorization'] = f"Bearer {auth_header[7:27]}...***"
            self.logger.debug(f"{search_id_prefix}Headers: {debug_headers}")
        
        if data:
            # Log size instead of full content to avoid massive logs with CSV data
            if hasattr(data, '__len__'):
                self.logger.debug(f"{search_id_prefix}Form Data: <{len(data)} bytes>")
            else:
                self.logger.debug(f"{search_id_prefix}Form Data: <data present, size unknown>")
        
        if json_data:
            # Sanitize sensitive JSON data
            debug_json = dict(json_data)
            if 'client_secret' in debug_json:
                debug_json['client_secret'] = '***REDACTED***'
            self.logger.debug(f"{search_id_prefix}JSON Data: {debug_json}")
        
        if response:
            self.logger.debug(f"{search_id_prefix}=== HTTP RESPONSE DEBUG ===")
            self.logger.debug(f"{search_id_prefix}Status Code: {response.status_code}")
            self.logger.debug(f"{search_id_prefix}Response Headers: {dict(response.headers)}")
            
            try:
                # Try to parse as JSON first
                response_json = response.json()
                
                # Sanitize sensitive fields in the response JSON
                debug_response_json = dict(response_json)
                if 'access_token' in debug_response_json:
                    token = debug_response_json['access_token']
                    # Show first 20 chars and last 4 chars with masking
                    if len(token) > 24:
                        debug_response_json['access_token'] = f"{token[:20]}...***{token[-4:]}"
                    else:
                        debug_response_json['access_token'] = "***REDACTED***"
                
                # Handle response based on debug level
                if self.http_debug_level == "full":
                    # Full level: always log complete response (legacy behavior)
                    self.logger.debug(f"{search_id_prefix}Response JSON: {debug_response_json}")
                else:
                    # Summary level: intelligent truncation for large responses
                    response_str = str(debug_response_json)
                    if len(response_str) > 5000:  # If response is very large
                        # Provide summary instead of full response
                        summary_info = []
                        
                        if isinstance(debug_response_json, dict):
                            for key, value in debug_response_json.items():
                                if isinstance(value, list):
                                    summary_info.append(f"{key}: [array with {len(value)} items]")
                                elif isinstance(value, dict):
                                    summary_info.append(f"{key}: {{object with {len(value)} keys}}")
                                elif isinstance(value, str) and len(value) > 100:
                                    summary_info.append(f"{key}: '{value[:50]}...' (truncated, {len(value)} chars)")
                                else:
                                    summary_info.append(f"{key}: {value}")
                            summary = "{" + ", ".join(summary_info) + "}"
                        elif isinstance(debug_response_json, list):
                            summary = f"[array with {len(debug_response_json)} items]"
                            if debug_response_json and len(debug_response_json) > 0:
                                first_item = debug_response_json[0]
                                if isinstance(first_item, dict):
                                    sample_keys = list(first_item.keys())[:5]  # Show first 5 keys
                                    if len(first_item.keys()) > 5:
                                        sample_keys.append("...")
                                    summary += f" - sample item keys: {sample_keys}"
                        else:
                            summary = f"<large response: {type(debug_response_json).__name__}, {len(response_str)} " \
                                      f"chars>"
                        
                        self.logger.debug(f"{search_id_prefix}Response JSON (summarized): {summary}")
                    else:
                        # Response is small enough to log in full
                        self.logger.debug(f"{search_id_prefix}Response JSON: {debug_response_json}")
            except:
                # If not JSON, log the raw text (truncated for large responses)
                response_text = response.text
                if len(response_text) > 2000:
                    self.logger.debug(f"{search_id_prefix}Response Text (truncated): {response_text[:2000]}..." \
                                      f"TRUNCATED")
                else:
                    self.logger.debug(f"{search_id_prefix}Response Text: {response_text}")
        
        self.logger.debug(f"{search_id_prefix}=== END HTTP DEBUG ===")
    

    def _make_http_request(self, method: str, url: str, headers: dict = None, data: dict = None, 
                           json_data: dict = None, **kwargs):
        """Make HTTP request with debugging."""
        # Log the request
        self._debug_http_request(method, url, headers, data, json_data)
        
        # Make the actual request
        response = requests.request(method, url, headers=headers, data=data, json=json_data, **kwargs)
        
        # Log the response
        self._debug_http_request(method, url, response=response)
        
        return response
    

    def authenticate(self, client_id: str, client_secret: str, cribl_cloud_url: str) -> bool:
        """
        Authenticate with Cribl Cloud and get bearer token.
        
        Args:
            client_id: OAuth Client ID
            client_secret: OAuth Client Secret  
            cribl_cloud_url: Full Cribl Cloud URL (e.g., https://main-org-id.cribl.cloud)
            
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            search_id_prefix = self._get_search_id_prefix()
            auth_url = "https://login.cribl.cloud/oauth/token"
            
            data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
                'audience': 'https://api.cribl.cloud'
            }
            
            # Log auth attempt without exposing credentials
            self.logger.debug(f"{search_id_prefix}Authentication URL: {auth_url}")
            auth_data_log = (
                f"Authentication data: {{'grant_type': 'client_credentials', "
                f"'client_id': '{client_id}', "
                f"'client_secret': '***{client_secret[-4:] if len(client_secret) > 4 else '***'}', "
                f"'audience': 'https://api.cribl.cloud'}}"
            )
            self.logger.debug(f"{search_id_prefix}{auth_data_log}")
            
            self.logger.info(f"{search_id_prefix}Authenticating with Cribl Cloud for client_id: {client_id}")
            
            response = self._make_http_request('POST', auth_url, json_data=data, timeout=30, verify=self.verify_ssl)
            
            self.logger.debug(f"{search_id_prefix} Authentication response status: {response.status_code}")
            self.logger.debug(f"{search_id_prefix} Authentication response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                token_data = response.json()
                self.logger.debug(f"{search_id_prefix} Token response keys: {list(token_data.keys())}")
                
                self.token = token_data.get('access_token')
                expires_in = token_data.get('expires_in', 86400)
                token_type = token_data.get('token_type', 'Bearer')
                
                # Log token info without exposing the actual token
                self.logger.debug(f"{search_id_prefix} Token length: {len(self.token) if self.token else 'None'}")
                self.logger.debug(f"{search_id_prefix} Token type: {token_type}")
                self.logger.debug(f"{search_id_prefix} Token prefix: {self.token[:20] if self.token else 'None'}...")
                
                self.logger.info(f"{search_id_prefix} Authentication successful. Token expires in {expires_in} seconds")
                return True
            else:
                response_text = response.text
                self.logger.error(f"{search_id_prefix} Authentication failed: {response.status_code} - {response_text}")
                
                try:
                    error_data = response.json()
                    self.logger.debug(f"{search_id_prefix} Error response JSON: {error_data}")
                except:
                    self.logger.debug(f"{search_id_prefix}Error response is not JSON: {response_text}")
                
                return False
                
        except Exception as e:
            search_id_prefix = self._get_search_id_prefix()
            self.logger.error(f"{search_id_prefix} Authentication error: {str(e)}")
            self.logger.debug(f"{search_id_prefix}Authentication exception type: {type(e)}")
            return False
    

    def get_lookup_config(self, cribl_cloud_url: str, workergroup: str, lookup: str) -> Optional[Dict]:
        """Fetch existing lookup configuration to preserve settings like mode."""
        search_id_prefix = self._get_search_id_prefix()
        
        if not self.token:
            self.logger.error(f"{search_id_prefix}No authentication token available")
            return None
            
        try:
            get_url = f"{cribl_cloud_url}/api/v1/m/{workergroup}/system/lookups/{lookup}"
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            self.logger.debug(f"{search_id_prefix}Getting existing lookup config for {lookup}")
            
            response = self._make_http_request('GET', get_url, headers=headers, timeout=30, verify=self.verify_ssl)
            
            if response.status_code == 200:
                config = response.json()
                self.logger.debug(f"{search_id_prefix}Retrieved lookup config: {config}")
                return config
            elif response.status_code == 404:
                self.logger.info(f"{search_id_prefix}Lookup {lookup} does not exist yet")
                return None
            else:
                err_msg = f"{response.status_code} - {response.text}"
                self.logger.error(f"{search_id_prefix}Failed to get lookup config: {err_msg}")
                return None
        except Exception as e:
            msg = f"Unable to retrieve existing lookup config (timeout/error): {str(e)}"
            self.logger.warning(f"{search_id_prefix}{msg}")
            self.logger.warning(f"{search_id_prefix}Will attempt to determine mode from upload response")
            return "timeout_error"  # Special marker to indicate we should check upload response
    

    def is_lookup_being_processed(self, cribl_cloud_url: str, workergroup: str, lookup: str) -> bool:
        """Check if lookup is being processed - API doesn't expose this directly."""
        search_id_prefix = self._get_search_id_prefix()
        
        # Try to get the lookup configuration - this should work if not processing
        config = self.get_lookup_config(cribl_cloud_url, workergroup, lookup)
        
        if not config:
            self.logger.debug(f"{search_id_prefix}Could not get lookup config for {lookup}, assuming not processing")
            return False
        
        # API doesn't provide processing status, rely on retry logic instead
        return False
    

    def wait_for_lookup_ready(
        self, 
        cribl_cloud_url: str, 
        workergroup: str, 
        lookup: str, 
        max_wait_time: int = 60
    ) -> bool:
        """Wait briefly before lookup operations to avoid processing conflicts."""
        import time
        search_id_prefix = self._get_search_id_prefix()
        
        wait_time = 2  # Brief pause to let any processing finish
        
        self.logger.debug(f"{search_id_prefix}Waiting {wait_time}s for lookup {lookup} to be ready")
        time.sleep(wait_time)
        
        return True  # Retry logic will handle any remaining conflicts


    def upload_lookup(self, cribl_cloud_url: str, workergroup: str, lookup: str, 
                     lookup_data: List[Dict], lookup_mode: str = "auto") -> Tuple[bool, Optional[str], Optional[str]]:
        """Upload lookup data to Cribl Cloud and configure mode."""
        search_id_prefix = self._get_search_id_prefix()
        
        if not self.token:
            self.logger.error(f"{search_id_prefix}No authentication token available")
            return False, None, None
            
        try:
            # Brief wait to avoid processing conflicts
            if not self.wait_for_lookup_ready(cribl_cloud_url, workergroup, lookup, max_wait_time=60):
                self.logger.error(f"{search_id_prefix}Lookup {lookup} is still being processed, cannot upload")
                return False, None, None
            
            existing_config = self.get_lookup_config(cribl_cloud_url, workergroup, lookup)
            
            # Convert lookup data to CSV format
            csv_content = self._convert_to_csv(lookup_data)
            
            # Step 1: PUT request to upload the file
            put_url = f"{cribl_cloud_url}/api/v1/m/{workergroup}/system/lookups?filename={lookup}"
            
            headers_put = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'text/csv; charset=utf-8'
            }
            
            self.logger.info(f"{search_id_prefix}Uploading lookup {lookup} to workergroup {workergroup}")
            
            # Encode CSV content as UTF-8 bytes to handle non-Latin characters
            csv_bytes = csv_content.encode('utf-8')
            self.logger.debug(f"{search_id_prefix}CSV content encoded as UTF-8, size: {len(csv_bytes)} bytes")
            
            # Calculate timeout based on file size - minimum 60 seconds, up to 10 minutes for very large files
            # Assume ~1MB per minute upload time plus extra buffer
            file_size_mb = len(csv_bytes) / (1024 * 1024)
            upload_timeout = max(60, min(600, int(file_size_mb * 60) + 120))
            msg = f"Using upload timeout: {upload_timeout}s for {file_size_mb:.2f}MB file"
            self.logger.debug(f"{search_id_prefix}{msg}")
            
            response = self._make_http_request('PUT', put_url, headers=headers_put, data=csv_bytes,
                                               timeout=upload_timeout, verify=self.verify_ssl)
            
            if response.status_code not in [200, 204]:
                err_msg = f"{response.status_code} - {response.text}"
                self.logger.error(f"{search_id_prefix}Failed to upload lookup: {err_msg}")
                return False, None, None
            
            # Extract temporary filename from response
            response_data = response.json()
            temp_filename = response_data.get('filename')
            
            if not temp_filename:
                self.logger.error(f"{search_id_prefix}No temporary filename returned from upload")
                return False, None, None
            
            # Step 2: PATCH request to finalize the lookup with retry logic
            return self._finalize_lookup_with_retry(
                cribl_cloud_url, workergroup, lookup, temp_filename, existing_config, lookup_mode
            )
            
        except Exception as e:
            self.logger.error(f"{search_id_prefix}Error uploading lookup: {str(e)}")
            return False, None, None


    def _finalize_lookup_with_retry(self, cribl_cloud_url: str, workergroup: str, lookup: str, 
                                  temp_filename: str, existing_config: Optional[Dict], 
                                  lookup_mode: str, max_retries: int = 5) -> Tuple[bool, Optional[str], Optional[str]]:
        """Finalize lookup upload with retry logic for processing conflicts."""
        import time
        search_id_prefix = self._get_search_id_prefix()
        
        patch_url = f"{cribl_cloud_url}/api/v1/m/{workergroup}/system/lookups/{lookup}"
        
        headers_patch = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        # Determine lookup mode based on parameters and existing config
        if lookup_mode == "auto" and existing_config and existing_config != "timeout_error":
            if 'items' in existing_config and len(existing_config['items']) > 0:
                current_mode = existing_config['items'][0].get('mode', 'memory')
            else:
                current_mode = 'memory'
            self.logger.debug(f"{search_id_prefix}Preserving existing lookup mode: {current_mode}")
        elif lookup_mode == "auto" and existing_config == "timeout_error":
            # We couldn't determine the existing mode due to timeout, try memory first
            # If that fails with "mode cannot be changed", we'll retry with disk mode
            current_mode = 'memory'
            self.logger.debug(f"{search_id_prefix}Timeout getting existing config, trying memory mode first")
        elif lookup_mode == "auto":
            current_mode = 'memory'
            self.logger.debug(f"{search_id_prefix}New lookup, defaulting to memory mode")
        elif lookup_mode == "memory":
            current_mode = 'memory'
            self.logger.debug(f"{search_id_prefix}Setting lookup mode to memory")
        elif lookup_mode == "disk":
            current_mode = 'disk'
            self.logger.debug(f"{search_id_prefix}Setting lookup mode to disk")
        else:
            current_mode = 'memory'  # fallback
            self.logger.info(f"{search_id_prefix}Unknown lookup mode '{lookup_mode}', defaulting to memory")
        
        patch_data = {
            "id": lookup,
            "fileInfo": {
                "filename": temp_filename
            },
            "mode": current_mode
        }
        
        # Keep existing settings like description, tags, etc.
        if existing_config and 'items' in existing_config and len(existing_config['items']) > 0:
            existing_item = existing_config['items'][0]
            for field in ['description', 'tags', 'ttl', 'fields']:
                if field in existing_item:
                    patch_data[field] = existing_item[field]
        
        # Track if we've tried changing the mode to avoid infinite retries
        mode_changed = False
        
        # Retry logic for processing conflicts
        for attempt in range(max_retries):
            self.logger.debug(f"{search_id_prefix}PATCH attempt {attempt + 1}/{max_retries}, data: {patch_data}")
            
            response = self._make_http_request(
                'PATCH',
                patch_url, 
                json_data=patch_data, 
                headers=headers_patch, 
                timeout=30, 
                verify=self.verify_ssl
            )
            
            if response.status_code in [200, 204]:
                self.logger.info(f"{search_id_prefix}Lookup {lookup} uploaded successfully with mode: {current_mode}")
                return True, temp_filename, current_mode
            
            # Check for processing conflict or deployment restriction
            if response.status_code == 400:
                try:
                    error_data = response.json()
                    error_message = error_data.get('message', '')
                    
                    if "Lookup is being processed" in error_message:
                        if attempt < max_retries - 1:
                            wait_time = 5 * (attempt + 1)  # Exponential backoff
                            self.logger.info(f"{search_id_prefix}Lookup {lookup} is being processed, retrying " +
                                                f"in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                            continue
                        else:
                            msg = f"Lookup {lookup} still being processed after {max_retries} attempts"
                            self.logger.error(f"{search_id_prefix}{msg}")
                            return False, None, None
                    elif "Lookup changes are restricted during config deployment" in error_message:
                        if attempt < max_retries - 1:
                            wait_time = 10 * (attempt + 1)  # Longer wait for deployment conflicts
                            self.logger.info(f"{search_id_prefix}Lookup changes restricted during config deployment, " +
                                                f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                            continue
                        else:
                            msg = f"Lookup {lookup} still restricted due to config deployment after {max_retries} " \
                                  f"attempts"
                            self.logger.error(f"{search_id_prefix}{msg}")
                            return False, None, None
                    elif "mode cannot be changed" in error_message.lower():
                        # Mode conflict - try switching to the opposite mode
                        if not mode_changed:
                            mode_changed = True
                            new_mode = "disk" if current_mode == "memory" else "memory"
                            msg = f"Mode conflict for lookup {lookup}. Switching {current_mode} to {new_mode}"
                            self.logger.info(f"{search_id_prefix}{msg}")
                            current_mode = new_mode
                            
                            # Update the patch data with the new mode
                            patch_data["mode"] = current_mode
                            continue
                        else:
                            msg = f"Mode conflict persists for lookup {lookup} after switching modes"
                            self.logger.error(f"{search_id_prefix}{msg}")
                            return False, None, None
                    elif "Lookup file" in error_message and "does not exist" in error_message:
                        # Lookup doesn't exist yet - try POST to create it
                        self.logger.info(f"{search_id_prefix}Lookup does not exist yet, trying POST to create it")
                        post_url = f"{cribl_cloud_url}/api/v1/m/{workergroup}/system/lookups"
                        try:
                            response = self._make_http_request(
                                'POST',
                                post_url, 
                                json_data=patch_data, 
                                headers=headers_patch, 
                                timeout=30, 
                                verify=self.verify_ssl
                            )
                            
                            if response.status_code in [200, 201, 204]:
                                self.logger.info(f"{search_id_prefix}Lookup {lookup} created successfully with POST")
                                return True, temp_filename, current_mode
                            else:
                                self.logger.error(f"{search_id_prefix}POST to create lookup failed: {response.status_code} - {response.text}")
                                return False, None, None
                        except Exception as e:
                            self.logger.error(f"{search_id_prefix}Error attempting POST to create lookup: {str(e)}")
                            return False, None, None
                except:
                    pass
            
            # Other errors
            self.logger.error(f"{search_id_prefix}Failed to finalize lookup: {response.status_code} - {response.text}")
            return False, None, None
        
        return False, None, None
    
    
    def commit_lookup(self, cribl_cloud_url: str, workergroup: str, lookup: str, commit_message: str = None, 
                     search_name: str = None, search_id: str = None) -> Tuple[bool, Optional[str]]:
        """Commit lookup changes to git with custom commit message."""
        search_id_prefix = self._get_search_id_prefix()
        
        if not self.token:
            self.logger.error(f"{search_id_prefix}No authentication token available")
            return False, None
            
        # Generate default commit message if none provided
        if not commit_message:
            # Create enhanced default message with search context
            search_context = ""
            if search_name and search_id:
                # Saved search: include both name and SID
                search_context = f" from saved search: \"{search_name}\" (SID: {search_id})"
            elif search_name:
                # Saved search without SID (fallback)
                search_context = f" from saved search: \"{search_name}\""
            elif search_id:
                # Ad-hoc search: just include SID
                search_context = f" from search (SID: {search_id})"
            else:
                # No context available
                search_context = " from splunk search"
            
            commit_message = f"Committing update of csv: \"{lookup}\" to workergroup: {workergroup}{search_context}"
            
        try:
            commit_url = f"{cribl_cloud_url}/api/v1/version/commit"
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            commit_data = {
                "message": commit_message,
                "group": workergroup,
                "files": [
                    f"groups/{workergroup}/data/lookups/{lookup}",
                    f"groups/{workergroup}/data/lookups/{lookup.replace('.csv', '.yml')}"
                ]
            }
            
            self.logger.info(f"{search_id_prefix}Committing lookup {lookup} for workergroup {workergroup}")
            self.logger.debug(f"{search_id_prefix}Commit message: {commit_message}")
            
            response = self._make_http_request('POST', commit_url, json_data=commit_data,
                                               headers=headers, timeout=30, verify=self.verify_ssl)
            
            if response.status_code not in [200, 201]:
                # Check if this is a gitignore-related error (which should be treated as success)
                if response.status_code == 500:
                    try:
                        error_data = response.json()
                        error_message = error_data.get('message', '')
                        
                        # Check for gitignore-related error patterns
                        gitignore_indicators = [
                            'ignored by one of your .gitignore files',
                            'hint: Use -f if you really want to add them',
                            'hint: Turn this message off by running'
                        ]
                        
                        if any(indicator in error_message for indicator in gitignore_indicators):
                            msg = "Lookup files ignored by .gitignore, upload/deploy successful"
                            self.logger.warning(f"{search_id_prefix}{msg}")
                            # Return success with a special commit hash indicating gitignore scenario
                            return True, "gitignore-ignored"
                    except (ValueError, KeyError):
                        # If we can't parse the JSON response, fall through to normal error handling
                        pass
                
                err_msg = f"{response.status_code} - {response.text}"
                self.logger.error(f"{search_id_prefix}Failed to commit lookup: {err_msg}")
                return False, None
            
            response_data = response.json()
            items = response_data.get('items', [])
            
            if not items:
                self.logger.error(f"{search_id_prefix}No commit information returned")
                return False, None
            
            commit_hash = items[0].get('commit')
            
            if not commit_hash:
                self.logger.error(f"{search_id_prefix}No commit hash returned")
                return False, None
            
            self.logger.info(f"{search_id_prefix}Lookup committed successfully with hash: {commit_hash}")
            return True, commit_hash
            
        except Exception as e:
            self.logger.error(f"{search_id_prefix}Error committing lookup: {str(e)}")
            return False, None


    def get_current_group_version(self, cribl_cloud_url: str, workergroup: str) -> Optional[str]:
        """Get the current effective configuration version for a worker group."""
        search_id_prefix = self._get_search_id_prefix()
        
        if not self.token:
            self.logger.error(f"{search_id_prefix}No authentication token available")
            return None
            
        try:
            # Use the documented API endpoint to get group information
            group_url = f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}"
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            self.logger.debug(f"{search_id_prefix}Getting current group version for {workergroup}")
            self.logger.debug(f"{search_id_prefix}Group URL: {group_url}")
            
            response = self._make_http_request('GET', group_url, headers=headers, timeout=30, verify=self.verify_ssl)
            
            self.logger.debug(f"{self._get_search_id_prefix()}Group version response status: {response.status_code}")
            
            if response.status_code == 200:
                group_data = response.json()
                self.logger.debug(f"{self._get_search_id_prefix()}Group data keys: {list(group_data.keys())}")
                self.logger.debug(f"{self._get_search_id_prefix()}Full group data: {group_data}")
                
                # Check for configVersion in items array (correct API format)
                current_version = None
                if 'items' in group_data and len(group_data['items']) > 0:
                    item = group_data['items'][0]
                    keys = list(item.keys())
                    self.logger.debug(f"{self._get_search_id_prefix()}Group items[0] keys: {keys}")
                    current_version = item.get('configVersion')
                    msg = f"Found configVersion in items[0]: {current_version}"
                    self.logger.debug(f"{self._get_search_id_prefix()}{msg}")
                else:
                    msg = "No 'items' array found in group data or items array is empty"
                    self.logger.debug(f"{search_id_prefix}{msg}")
                
                # Fallback to try multiple possible version fields at root level
                if not current_version:
                    self.logger.debug(f"{search_id_prefix}Trying fallback version fields at root level")
                    fallback_fields = ['configVersion', 'version', 'currentVersion', 'activeVersion']
                    for field in fallback_fields:
                        value = group_data.get(field)
                        self.logger.debug(f"{search_id_prefix}Checking field '{field}': {value}")
                        if value:
                            current_version = value
                            msg = f"Found version in fallback '{field}': {current_version}"
                            self.logger.debug(f"{search_id_prefix}{msg}")
                            break
                
                if current_version:
                    self.logger.info(f"{search_id_prefix}Current effective version for group {workergroup}: " \
                                     f"{current_version}")
                    return current_version
                else:
                    self.logger.warning(f"{search_id_prefix}No version information found in group data for " \
                                        f"{workergroup}")
                    self.logger.debug(f"{search_id_prefix}Available fields in group data: {list(group_data.keys())}")
                    if 'items' in group_data and len(group_data['items']) > 0:
                        self.logger.debug(f"{search_id_prefix}Available fields in items[0]: " \
                                          f"{list(group_data['items'][0].keys())}")
                    return None
                    
            else:
                error_text = response.text
                self.logger.error(f"{search_id_prefix}Failed to get group version: {response.status_code} - " \
                                  f"{error_text}")
                self.logger.debug(f"{search_id_prefix}Group version error response headers: {dict(response.headers)}")
                return None
                
        except Exception as e:
            self.logger.error(f"{search_id_prefix}Error getting group version: {str(e)}")
            self.logger.debug(f"{search_id_prefix}Exception type: {type(e)}")
            import traceback
            self.logger.debug(f"{search_id_prefix}Full traceback: {traceback.format_exc()}")
            return None


    def deploy_lookup(self, cribl_cloud_url: str, workergroup: str, commit_hash: str, progress_callback=None) -> bool:
        """Deploy committed lookup changes with polling for completion."""
        search_id_prefix = self._get_search_id_prefix()
        
        if not self.token:
            self.logger.error(f"{search_id_prefix}No authentication token available")
            return False
            
        try:
            self.logger.debug(f"{search_id_prefix}Starting deploy_lookup with commit_hash: {commit_hash}")
            
            # Handle gitignore scenario where commit failed but upload succeeded
            deploy_version = commit_hash
            if commit_hash == "gitignore-ignored":
                self.logger.info(f"{search_id_prefix}Commit was blocked by gitignore, getting current effective " \
                                 f"version for deploy")
                self.logger.debug(f"{search_id_prefix}Original commit_hash was: {commit_hash}")
                
                current_version = self.get_current_group_version(cribl_cloud_url, workergroup)
                if current_version:
                    deploy_version = current_version
                    self.logger.info(f"{search_id_prefix}Using current effective version for deploy: {deploy_version}")
                    self.logger.debug(f"{search_id_prefix}Successfully retrieved current version: {current_version}")
                else:
                    self.logger.error(f"{search_id_prefix}Could not determine current version for gitignore scenario")
                    self.logger.debug(f"{search_id_prefix}get_current_group_version returned None")
                    return False
            else:
                self.logger.debug(f"{search_id_prefix}Normal commit scenario, using commit_hash as deploy_version: " \
                                  f"{deploy_version}")
            
            deploy_url = f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}/deploy"
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            deploy_data = {"version": deploy_version}
            
            self.logger.debug(f"{self._get_search_id_prefix()}Deploy URL: {deploy_url}")
            self.logger.debug(f"{self._get_search_id_prefix()}Deploy data: {deploy_data}")
            self.logger.debug(f"{self._get_search_id_prefix()}Deploy headers (auth masked): {{'Authorization': " \
                              f"'Bearer ***...', 'Content-Type': '{headers['Content-Type']}'}}")
            self.logger.info(f"{self._get_search_id_prefix()}Initiating deployment of version {deploy_version} to " \
                             f"workergroup {workergroup}")
            
            # Send the deployment request
            response = self._make_http_request(
                'PATCH',
                deploy_url, 
                json_data=deploy_data, 
                headers=headers, 
                verify=self.verify_ssl,
                timeout=30
            )
            
            self.logger.debug(f"{self._get_search_id_prefix()}Deploy request response status: {response.status_code}")
            
            # Log response details for troubleshooting
            if response.status_code != 200 and response.status_code != 204:
                try:
                    error_details = response.json()
                    self.logger.debug(f"{self._get_search_id_prefix()}Deploy error response JSON: {error_details}")
                except:
                    self.logger.debug(f"{self._get_search_id_prefix()}Deploy error response text: {response.text}")
                    
            # Handle immediate response - both success and "work in progress" scenarios
            if response.status_code in [200, 204]:
                self.logger.info(f"{self._get_search_id_prefix()}Deploy request accepted by Cribl API")
                self.logger.debug(f"{self._get_search_id_prefix()}Deploy accepted with status {response.status_code}" \
                                  f", starting polling")
                # For successful responses, use 60 seconds to allow sufficient time for deployment
                return self._poll_deployment_status(cribl_cloud_url, workergroup, deploy_version, max_wait_time=60, 
                                                    progress_callback=progress_callback)
            elif response.status_code == 500:
                # 500 might indicate async operation in progress
                self.logger.debug(f"{self._get_search_id_prefix()}Received 500 status code, checking error details")
                try:
                    error_details = response.json()
                    error_msg = error_details.get('message', 'Unknown server error')
                    self.logger.debug(f"{self._get_search_id_prefix()}500 error details: {error_details}")
                    
                    if 'Lookups not processed' in error_msg:
                        self.logger.info(f"{self._get_search_id_prefix()}Deploy request triggered async operation " \
                                         f"(500 with 'Lookups not processed')")
                        self.logger.info(f"{self._get_search_id_prefix()}This is normal - deployment will continue in" \
                                          f" background")
                        self.logger.debug(f"{self._get_search_id_prefix()}Starting extended polling for 500 response")
                    else:
                        self.logger.info(f"{self._get_search_id_prefix()}Deploy request returned 500: {error_msg}")
                        self.logger.debug(f"{self._get_search_id_prefix()}500 error message: {error_msg}")
                        
                except ValueError as e:
                    self.logger.info(f"{self._get_search_id_prefix()}Deploy request returned 500 with unparseable " \
                                     f"response")
                    self.logger.debug(f"{self._get_search_id_prefix()}JSON parse error: {e}")
                    self.logger.debug(f"{self._get_search_id_prefix()}Raw 500 response: {response.text}")
                    
                # For 500 responses, use longer polling time as operation might take longer
                self.logger.debug(f"{self._get_search_id_prefix()}Using 90 second timeout for 500 response polling")
                return self._poll_deployment_status(cribl_cloud_url, workergroup, deploy_version, max_wait_time=90, 
                                                    progress_callback=progress_callback)
            elif response.status_code == 400:
                # 400 errors are usually configuration issues
                self.logger.error(f"{self._get_search_id_prefix()}Deploy request returned 400 Bad Request")
                try:
                    error_details = response.json()
                    error_msg = error_details.get('message', 'Unknown client error')
                    self.logger.error(f"{self._get_search_id_prefix()}400 error message: {error_msg}")
                    self.logger.debug(f"{self._get_search_id_prefix()}400 error details: {error_details}")
                    
                    # Check for specific error patterns
                    if "not the current effective config version" in error_msg:
                        self.logger.error(f"{self._get_search_id_prefix()}Version mismatch error: requested version " \
                                          f"'{deploy_version}' is not current")
                        self.logger.debug(f"{self._get_search_id_prefix()}This suggests the version we tried to " \
                                          f"deploy ({deploy_version}) is outdated")
                    
                except ValueError:
                    self.logger.error(f"{self._get_search_id_prefix()}400 error with unparseable response: " \
                                      f"{response.text}")
                    
                return False
            else:
                self.logger.info(f"{self._get_search_id_prefix()}Deploy request returned unexpected status " \
                                 f"{response.status_code}")
                self.logger.debug(f"{self._get_search_id_prefix()}Unexpected status response text: {response.text}")
                # Still try polling even for unexpected status codes
                self.logger.debug(f"{self._get_search_id_prefix()}Attempting polling despite unexpected status")
                return self._poll_deployment_status(cribl_cloud_url, workergroup, deploy_version, max_wait_time=60, 
                                                    progress_callback=progress_callback)
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"{self._get_search_id_prefix()}Network error during deploy request: {e}")
            self.logger.debug(f"{self._get_search_id_prefix()}Request exception type: {type(e)}")
            import traceback
            self.logger.debug(f"{self._get_search_id_prefix()}Request exception traceback: {traceback.format_exc()}")
            return False
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()}Deploy operation failed: {e}")
            self.logger.debug(f"{self._get_search_id_prefix()}General exception type: {type(e)}")
            import traceback
            self.logger.debug(f"{self._get_search_id_prefix()}General exception traceback: {traceback.format_exc()}")
            return False


    def _poll_deployment_status(self, cribl_cloud_url: str, 
                                    workergroup: str, 
                                    commit_hash: str, 
                                    max_wait_time: int = 60, 
                                    poll_interval: int = 3, 
                                    progress_callback=None
                                ) -> bool:
        """
        Poll the deployment status until completion or timeout.
        
        Args:
            cribl_cloud_url: Full Cribl Cloud URL
            workergroup: Target worker group name  
            commit_hash: Commit hash being deployed
            max_wait_time: Maximum time to wait in seconds
            poll_interval: Time between status checks in seconds
            
        Returns:
            bool: True if deployment succeeded, False otherwise
        """
        if not self.token:
            self.logger.error(f"{self._get_search_id_prefix()}No authentication token available for status polling")
            return False
            
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        # Check current deployment status endpoint
        status_url = f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}/deploy"
        
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            try:
                self.logger.debug(f"{self._get_search_id_prefix()}Checking deployment status (elapsed: " \
                                  f"{elapsed_time}s, target: {commit_hash})")
                self.logger.info(f"{self._get_search_id_prefix()}Deployment status check: elapsed {elapsed_time}s" \
                                  f" of {max_wait_time}s, target commit: {commit_hash}")
                
                # Call progress callback if provided
                if progress_callback:
                    progress_callback({
                        'status': 'deployment_polling',
                        'elapsed_time': elapsed_time,
                        'max_wait_time': max_wait_time,
                        'commit_hash': commit_hash,
                        'workergroup': workergroup,
                        'message': f'Waiting for deployment to complete ({elapsed_time}s/{max_wait_time}s)'
                    })
                
                # Get current deployment status
                response = self._make_http_request('GET', 
                                                    status_url, 
                                                    headers=headers, 
                                                    verify=self.verify_ssl, 
                                                    timeout=10
                                                   )
                
                if response.status_code == 200:
                    try:
                        status_data = response.json()
                        self.logger.debug(f"{self._get_search_id_prefix()}Deployment status response: {status_data}")
                        
                        # Check if our commit hash is deployed (try both full and short versions)
                        current_version = status_data.get('version')
                        if current_version:
                            # Check both exact match and short hash match (first 7 chars)
                            commit_short = commit_hash[:7] if len(commit_hash) > 7 else commit_hash
                            current_short = current_version[:7] if len(current_version) > 7 else current_version
                            
                            if current_version == commit_hash or current_short == commit_short:
                                self.logger.info(f"{self._get_search_id_prefix()}Deployment confirmed! Version " \
                                                 f"{current_version} matches target {commit_hash}")
                                return True
                            else:
                                self.logger.debug(f"{self._get_search_id_prefix()}Current version: {current_version} " \
                                                  f"(short: {current_short}), waiting for: {commit_hash} " \
                                                  f"(short: {commit_short})")
                        else:
                            self.logger.debug(f"{self._get_search_id_prefix()}No version field in deployment status" \
                                               f" response")
                            
                    except ValueError as e:
                        self.logger.debug(f"{self._get_search_id_prefix()}Could not parse status response: {e}")
                        self.logger.debug(f"{self._get_search_id_prefix()}Raw response: {response.text[:200]}...")
                        
                elif response.status_code == 404:
                    self.logger.debug(f"{self._get_search_id_prefix()}Deployment status endpoint not available, " \
                                      f"trying alternative check")
                else:
                    self.logger.debug(f"{self._get_search_id_prefix()}Deployment status returned " \
                                      f"{response.status_code}: {response.text[:200]}...")
                    
                # Alternative check: try to get worker group info to see current version
                alt_status_url = f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}"
                alt_response = self._make_http_request('GET', 
                                                        alt_status_url, 
                                                        headers=headers, 
                                                        verify=self.verify_ssl, 
                                                        timeout=10
                                                       )
                
                if alt_response.status_code == 200:
                    try:
                        group_data = alt_response.json()
                        self.logger.debug(f"{self._get_search_id_prefix()}Worker group data keys:" \
                                           f" {list(group_data.keys())}")
                        
                        # Check for configVersion in items array (correct API format)
                        deployed_version = None
                        if 'items' in group_data and len(group_data['items']) > 0:
                            item = group_data['items'][0]
                            deployed_version = item.get('configVersion')
                            self.logger.debug(f"{self._get_search_id_prefix()}Found configVersion in items[0]: " \
                                              f"{deployed_version}")
                        
                        # Fallback to try multiple possible version fields at root level
                        if not deployed_version:
                            deployed_version = (group_data.get('deployedVersion') or 
                                              group_data.get('version') or 
                                              group_data.get('currentVersion') or
                                              group_data.get('activeVersion') or
                                              group_data.get('configVersion'))
                        
                        if deployed_version:
                            # Check both exact match and short hash match
                            commit_short = commit_hash[:7] if len(commit_hash) > 7 else commit_hash
                            deployed_short = deployed_version[:7] if len(deployed_version) > 7 else deployed_version
                            
                            if deployed_version == commit_hash or deployed_short == commit_short:
                                self.logger.info(f"{self._get_search_id_prefix()} Deployment confirmed via group " \
                                                 f"status! Version {deployed_version} matches target {commit_hash}")
                                return True
                            else:
                                self.logger.debug(f"{self._get_search_id_prefix()}Group deployed version: " \
                                                  f"{deployed_version} (short: {deployed_short}), waiting for: " \
                                                  f"{commit_hash} (short: {commit_short})")
                        else:
                            self.logger.debug(f"{self._get_search_id_prefix()}No version field found in group data")
                            
                    except ValueError as e:
                        self.logger.debug(f"{self._get_search_id_prefix()} Could not parse group status response: {e}")
                        self.logger.debug(f"{self._get_search_id_prefix()} Raw group response: " \
                                          f"{alt_response.text[:200]}...")
                else:
                    self.logger.debug(f"{self._get_search_id_prefix()} Group status returned " \
                                      f"{alt_response.status_code}: {alt_response.text[:200]}...")
                
                # Call progress callback to prevent auto-cancellation
                if progress_callback:
                    progress_callback(f"Polling deployment status: {elapsed_time}s/{max_wait_time}s")
                
                # Wait before next poll
                time.sleep(poll_interval)
                elapsed_time += poll_interval
                
            except requests.exceptions.RequestException as e:
                self.logger.debug(f"{self._get_search_id_prefix()} Network error during status check: {e}")
                
                # Call progress callback even during errors
                if progress_callback:
                    progress_callback(f"Network error during polling, retrying: {elapsed_time}s/{max_wait_time}s")
                
                time.sleep(poll_interval)
                elapsed_time += poll_interval
                continue
                
        # Timeout reached
        self.logger.info(f"{self._get_search_id_prefix()} Deployment status polling timed out after {max_wait_time}s")
        self.logger.info(f"{self._get_search_id_prefix()} This doesn't necessarily mean deployment failed - it " \
                         f"may still be in progress")
        
        # One final check " \
                     #"attempt after timeout
        self.logger.info(f"{self._get_search_id_prefix()}Performing final deployment verification...")
        try:
            final_check_url = f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}"
            final_response = self._make_http_request('GET', 
                                                        final_check_url, 
                                                        headers=headers, 
                                                        verify=self.verify_ssl, 
                                                        timeout=10
                                                    )
            
            if final_response.status_code == 200:
                group_data = final_response.json()
                deployed_version = (group_data.get('deployedVersion') or 
                                    group_data.get('version') or 
                                    group_data.get('currentVersion') or
                                    group_data.get('activeVersion')
                                   )
                
                if deployed_version:
                    commit_short = commit_hash[:7] if len(commit_hash) > 7 else commit_hash
                    deployed_short = deployed_version[:7] if len(deployed_version) > 7 else deployed_version
                    
                    if deployed_version == commit_hash or deployed_short == commit_short:
                        self.logger.info(f"{self._get_search_id_prefix()}Final check SUCCESS! Deployment " \
                                         f"{deployed_version} confirmed after timeout")
                        return True
                        
        except Exception as e:
            self.logger.debug(f"{self._get_search_id_prefix()}Final check failed: {e}")
        
        self.logger.warning(f"{self._get_search_id_prefix()}Deployment status could not be confirmed within " \
                            f"timeout period")
        self.logger.info(f"{self._get_search_id_prefix()}Check Cribl UI to verify deployment status manually")  
        # Return False when we can't confirm deployment success
        return False
    

    def deploy_lookup_with_version(
        self, 
        cribl_cloud_url: str, 
        workergroup: str, 
        version_id: Optional[str] = None
    ) -> bool:
        """
        Deploy changes using the current version ID
        
        This method can be used as an alternative deployment approach
        when a specific version ID is known.
        
        Args:
            cribl_cloud_url: Full Cribl Cloud URL
            workergroup: Target worker group name
            version_id: Optional version ID from lookup config, if available
            
        Returns:
            bool: True if deployment successful, False otherwise
        """
        if not self.token:
            self.logger.error(f"{self._get_search_id_prefix()}No authentication token available")
            return False
            
        try:
            deploy_url = f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}/deploy"
            
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            # Use version ID if available, otherwise deploy current state
            if version_id:
                deploy_data = {"version": version_id}
                self.logger.info(f"{self._get_search_id_prefix()}Deploying version {version_id} to workergroup " \
                                 f"{workergroup}")
            else:
                deploy_data = {}  # Empty payload to deploy current state
                self.logger.info(f"{self._get_search_id_prefix()}Deploying current state to workergroup {workergroup}")
            
            response = self._make_http_request('PATCH', 
                                               deploy_url, 
                                               json_data=deploy_data, 
                                               headers=headers, 
                                               timeout=60, 
                                               verify=self.verify_ssl)
            
            if response.status_code not in [200, 204]:
                self.logger.error(f"{self._get_search_id_prefix()}Deploy failed: " \
                                  f"{response.status_code} - {response.text}")
                return False
            
            self.logger.info(f"{self._get_search_id_prefix()}Lookup deployed successfully to workergroup {workergroup}")
            return True
            
        except Exception as e:
            self.logger.error(f"{self._get_search_id_prefix()}Error deploying lookup: {str(e)}")
            return False
    
    
    def sync_lookup(self, leader_config: Dict, workergroup: str, lookup: str, 
                   lookup_data: List[Dict], lookup_mode: str = "auto", commit_message: str = None,
                   search_name: str = None, search_id: str = None, progress_callback=None) -> bool:
        """Complete sync operation with retry logic and custom commit message."""
        try:
            # Set current search ID for logging context
            self.current_search_id = search_id
            search_id_prefix = self._get_search_id_prefix()
            
            self.logger.info(f"{search_id_prefix}Starting sync for lookup '{lookup}' to workergroup '{workergroup}'")
            self.logger.debug(f"{search_id_prefix}Lookup mode: {lookup_mode}, Records: {len(lookup_data)}")
            if commit_message:
                self.logger.debug(f"{search_id_prefix}Custom commit message: {commit_message}")
            if search_name:
                self.logger.debug(f"{search_id_prefix}Search name: {search_name}")
            elif search_id:
                self.logger.debug(f"{search_id_prefix}Search ID: {search_id}")
            
            # Step 1: Authenticate with retry
            auth_success = self._retry_with_backoff(
                "Authentication",
                    self.authenticate,
                leader_config['client_id'],
                leader_config['client_secret'], 
                leader_config['cribl_cloud_url'],
                progress_callback=progress_callback
            )
            
            if not auth_success:
                self.logger.error(f"{search_id_prefix}Authentication failed after retries")
                return False

            # Upload lookup with retry           
            upload_result = self._retry_with_backoff(
                "Lookup Upload",
                self.upload_lookup,
                leader_config['cribl_cloud_url'],
                workergroup,
                lookup,
                lookup_data,
                lookup_mode,
                progress_callback=progress_callback
            )
            
            success, temp_filename, actual_mode = upload_result
            
            if not success:
                self.logger.error(f"{search_id_prefix}Lookup upload failed after retries")
                return False
            
            self.logger.info(f"{search_id_prefix}Upload successful. Mode: {actual_mode}, Temp file: {temp_filename}")
            
            # Commit changes to git
            commit_result = self._retry_with_backoff(
                "Lookup Commit",
                self.commit_lookup,
                leader_config['cribl_cloud_url'],
                workergroup,
                lookup,
                commit_message,
                search_name,
                search_id,
                progress_callback=progress_callback
            )
            
            commit_success, commit_hash = commit_result
            
            # Log commit result but continue to deploy regardless
            if commit_success:
                if commit_hash == "gitignore-ignored":
                    self.logger.info(f"{search_id_prefix}Commit blocked by gitignore rules, but upload succeeded")
                else:
                    self.logger.info(f"{search_id_prefix}Commit successful. Hash: {commit_hash}")
            else:
                self.logger.warning(f"{search_id_prefix}Lookup commit failed, but proceeding with deploy step")
                # When commit fails, we need to get the current group version for deployment
                if commit_hash is None:
                    self.logger.info(f"{search_id_prefix}Commit failed - getting current effective version for deploy")
                    current_version = self.get_current_group_version(leader_config['cribl_cloud_url'], workergroup)
                    if current_version:
                        commit_hash = current_version
                        self.logger.info(f"{search_id_prefix}Using current effective version for deploy: {commit_hash}")
                    else:
                        self.logger.error(f"{search_id_prefix}Could not determine current version after commit failure")
                        return False
            
            # Deploy to worker group - this is required regardless of commit status
            # as it pushes the lookup changes to worker nodes
            deploy_result = False
            start_time = time.time()
            attempt = 1
            
            while time.time() - start_time < self.deploy_max_wait_time:
                try:
                    self.logger.debug(f"{search_id_prefix}Deploy attempt {attempt} (elapsed: " \
                                      f"{time.time() - start_time:.1f}s/{self.deploy_max_wait_time}s)")
                    deploy_result = self._deploy_with_extended_wait(
                        leader_config['cribl_cloud_url'],
                        workergroup,
                        commit_hash,
                        progress_callback=progress_callback
                    )
                    if deploy_result:
                        if attempt > 1:
                            self.logger.info(f"{search_id_prefix}Deploy succeeded on attempt {attempt} after " \
                                             f"{time.time() - start_time:.1f}s")
                        break
                    else:
                        if time.time() - start_time < self.deploy_max_wait_time:
                            # Calculate exponential backoff delay, but cap it at deploy_retry_interval * 4
                            delay = min(self.deploy_retry_interval * (1.5 ** (attempt - 1)),
                                        self.deploy_retry_interval * 4)
                            delay += random.uniform(0, 5)  
                            
                            remaining_time = self.deploy_max_wait_time - (time.time() - start_time)
                            if delay > remaining_time:
                                delay = remaining_time
                                
                            self.logger.info(f"{search_id_prefix}Deploy attempt {attempt} failed, retrying in " \
                                             f"{delay:.1f}s (remaining: {remaining_time:.1f}s)")
                            
                            # Send progress update during deployment retry delay
                            if progress_callback:
                                progress_callback(f"Deploy retry {attempt + 1} in {delay:.1f}s (elapsed: " \
                                                  f"{time.time() - start_time:.1f}s)")
                            
                            time.sleep(delay)
                            attempt += 1
                except Exception as e:
                    self.logger.error(f"{search_id_prefix}Deploy attempt {attempt} failed with error: {e}")
                    remaining_time = self.deploy_max_wait_time - (time.time() - start_time)
                    if remaining_time > 0:
                        delay = min(self.deploy_retry_interval * (1.5 ** (attempt - 1)), self.deploy_retry_interval * 4)
                        delay += random.uniform(0, 5)
                        if delay > remaining_time:
                            delay = remaining_time
                        self.logger.info(f"{search_id_prefix}Retrying deploy in {delay:.1f}s")
                        time.sleep(delay)
                        attempt += 1
                    else:
                        raise
            
            if not deploy_result:
                elapsed_time = time.time() - start_time
                self.logger.error(f"{search_id_prefix}Lookup deploy failed after {elapsed_time:.1f}s timeout " \
                                  f"(max: {self.deploy_max_wait_time}s)")
                return False
            
            self.logger.info(f"{search_id_prefix}Deploy successful")
            
            # Create appropriate success message based on commit/deploy results
            if commit_success:
                if commit_hash == "gitignore-ignored":
                    success_details = "upload and deploy succeeded (commit blocked by gitignore)"
                else:
                    success_details = "upload, commit, and deploy all succeeded"
            else:
                success_details = "upload and deploy succeeded (commit failed)"
                
            self.logger.info(
                f"{search_id_prefix}Complete sync operation successful for {lookup} to {workergroup} "
                f"({actual_mode}-based lookup: {success_details})"
            )
            
            return True
            
        except Exception as e:
            search_id_prefix = self._get_search_id_prefix()
            self.logger.error(f"{search_id_prefix}Sync operation failed: {str(e)}")
            return False


    def sync_lookup_with_progress(
                                    self, 
                                    leader_config: Dict, 
                                    workergroup: str, 
                                    lookup: str, 
                                    lookup_data: List[Dict], 
                                    lookup_mode: str = "auto", 
                                    commit_message: str = None,
                                    search_name: str = None, 
                                    search_id: str = None
                                ):
        """Generator-based sync operation that yields progress messages."""
        try:
            # Set search ID for debug logging context
            if search_id:
                self.current_search_id = search_id
            
            yield {'type': 'progress', 
                   'message': f"Starting sync for lookup '{lookup}' to workergroup '{workergroup}'"}
            self.logger.info(f"{self._get_search_id_prefix()}Starting sync for lookup '{lookup}' to workergroup " \
                             f"'{workergroup}'")
            self.logger.debug(f"{self._get_search_id_prefix()}Lookup mode: {lookup_mode}, Records: {len(lookup_data)}")
            if commit_message:
                self.logger.debug(f"{self._get_search_id_prefix()}Custom commit message: {commit_message}")
            if search_name:
                self.logger.debug(f"{self._get_search_id_prefix()}Search name: {search_name}")
            elif search_id:
                self.logger.debug(f"{self._get_search_id_prefix()}Search ID: {search_id}")
            
            # Step 1: Authenticate with retry
            yield {'type': 'progress', 'message': 'Authentication: Attempt 1/3'}
            auth_success = self._retry_with_backoff_generator(
                "Authentication",
                self.authenticate,
                leader_config['client_id'],
                leader_config['client_secret'], 
                leader_config['cribl_cloud_url']
            )
            
            for auth_result in auth_success:
                if auth_result.get('type') == 'progress':
                    yield auth_result
                elif auth_result.get('type') == 'success':
                    break
                elif auth_result.get('type') == 'error':
                    self.logger.error(f"{self._get_search_id_prefix()}Authentication failed after retries")
                    yield {'type': 'error', 'message': 'Authentication failed after retries'}
                    return

            # Upload lookup with retry
            yield {'type': 'progress', 'message': 'Lookup Upload: Attempt 1/3'}
            upload_generator = self._retry_with_backoff_generator(
                "Lookup Upload",
                self.upload_lookup,
                leader_config['cribl_cloud_url'],
                workergroup,
                lookup,
                lookup_data,
                lookup_mode
            )
            
            upload_result = None
            for upload_progress in upload_generator:
                if upload_progress.get('type') == 'progress':
                    yield upload_progress
                elif upload_progress.get('type') == 'success':
                    upload_result = upload_progress.get('result')
                    break
                elif upload_progress.get('type') == 'error':
                    self.logger.error(f"{self._get_search_id_prefix()}Lookup upload failed after retries")
                    yield {'type': 'error', 'message': 'Lookup upload failed after retries'}
                    return
            
            success, temp_filename, actual_mode = upload_result
            
            if not success:
                self.logger.error(f"{self._get_search_id_prefix()}Lookup upload failed after retries")
                yield {'type': 'error', 'message': 'Lookup upload failed after retries'}
                return
            
            self.logger.info(f"{self._get_search_id_prefix()}Upload successful. Mode: {actual_mode}, Temp " \
                             f"file: {temp_filename}")
            yield {'type': 'progress', 'message': f'Upload successful. Mode: {actual_mode}, Temp file: {temp_filename}'}
            
            # Commit changes to git
            yield {'type': 'progress', 'message': 'Lookup Commit: Attempt 1/3'}
            commit_generator = self._retry_with_backoff_generator(
                "Lookup Commit",
                self.commit_lookup,
                leader_config['cribl_cloud_url'],
                workergroup,
                lookup,
                commit_message,
                search_name,
                search_id
            )
            
            commit_result = None
            for commit_progress in commit_generator:
                if commit_progress.get('type') == 'progress':
                    yield commit_progress
                elif commit_progress.get('type') == 'success':
                    commit_result = commit_progress.get('result')
                    break
                elif commit_progress.get('type') == 'error':
                    search_id_prefix = self._get_search_id_prefix()
                    self.logger.warning(f"{search_id_prefix}Lookup commit failed after retries, but continuing " \
                                        f"with deploy")
                    yield {'type': 'progress', 
                           'message': 'Lookup commit failed after retries, but continuing with deploy'}
                    # Set default result for failed commit
                    commit_result = (False, None)
                    break
            
            # Handle case where no result was set
            if commit_result is None:
                search_id_prefix = self._get_search_id_prefix()
                self.logger.warning(f"{search_id_prefix}Commit operation did not complete, using defaults")
                commit_result = (False, None)
                
            commit_success, commit_hash = commit_result
            search_id_prefix = self._get_search_id_prefix()
            
            # Log commit result but continue to deploy regardless
            if commit_success:
                if commit_hash == "gitignore-ignored":
                    commit_msg = "Commit blocked by gitignore rules, but upload succeeded"
                    self.logger.info(f"{search_id_prefix}{commit_msg}")
                    yield {'type': 'progress', 'message': commit_msg}
                else:
                    commit_msg = f"Commit successful. Hash: {commit_hash}"
                    self.logger.info(f"{search_id_prefix}{commit_msg}")
                    yield {'type': 'progress', 'message': commit_msg}
            else:
                commit_msg = "Lookup commit failed, but proceeding with deploy step"
                self.logger.warning(f"{search_id_prefix}{commit_msg}")
                yield {'type': 'progress', 'message': commit_msg}
                # When commit fails, we need to get the current group version for deployment
                if commit_hash is None:
                    self.logger.info(f"{search_id_prefix}Commit failed - getting current effective version for deploy")
                    yield {'type': 'progress', 
                           'message': 'Commit failed - getting current effective version for deploy'}
                    current_version = self.get_current_group_version(leader_config['cribl_cloud_url'], workergroup)
                    if current_version:
                        commit_hash = current_version
                        self.logger.info(f"{search_id_prefix}Using current effective version for deploy: {commit_hash}")
                        yield {'type': 'progress', 
                               'message': f'Using current effective version for deploy: {commit_hash}'}
                    else:
                        self.logger.error(f"{search_id_prefix}Could not determine current version after commit failure")
                        yield {'type': 'error', 
                               'message': 'Could not determine current version after commit failure'}
                        return
            
            # Deploy to worker group - this is required regardless of commit status
            # as it pushes the lookup changes to worker nodes
            deploy_result = False
            start_time = time.time()
            attempt = 1
            
            while time.time() - start_time < self.deploy_max_wait_time:
                try:
                    elapsed_time = time.time() - start_time
                    remaining_time = self.deploy_max_wait_time - elapsed_time
                    msg = f"Deploy attempt {attempt} (elapsed: {elapsed_time:.1f}s, remaining: {remaining_time:.1f}s)"
                    self.logger.debug(f"{search_id_prefix} {msg}")
                    yield {'type': 'progress', 'message': msg}
                    
                    deploy_generator = self._deploy_with_extended_wait_generator(
                        leader_config['cribl_cloud_url'],
                        workergroup,
                        commit_hash
                    )
                    
                    for deploy_progress in deploy_generator:
                        if deploy_progress.get('type') == 'progress':
                            yield deploy_progress
                        elif deploy_progress.get('type') == 'success':
                            deploy_result = True
                            break
                        elif deploy_progress.get('type') == 'error':
                            break
                    
                    if deploy_result:
                        if attempt > 1:
                            self.logger.info(f"{search_id_prefix}Deploy succeeded on attempt {attempt} " \
                                             f"after {elapsed_time:.1f}s")
                        break
                    else:
                        remaining_time = self.deploy_max_wait_time - (time.time() - start_time)
                        if remaining_time > 0:
                            # Calculate exponential backoff delay, but cap it at deploy_retry_interval * 4
                            delay = min(self.deploy_retry_interval * (1.5 ** (attempt - 1)), 
                                        self.deploy_retry_interval * 4)
                            delay += random.uniform(0, 5)  
                            
                            if delay > remaining_time:
                                delay = remaining_time
                            
                            msg = f"Deploy attempt {attempt} failed, retrying in {delay:.1f}s (remaining: " \
                                  f"{remaining_time:.1f}s)"
                            self.logger.info(f"{search_id_prefix} {msg}")
                            yield {'type': 'progress', 
                                   'message': msg}
                            time.sleep(delay)
                            attempt += 1
                except Exception as e:
                    self.logger.error(f"{search_id_prefix}Deploy attempt {attempt} failed with error: {e}")
                    remaining_time = self.deploy_max_wait_time - (time.time() - start_time)
                    if remaining_time > 0:
                        delay = min(self.deploy_retry_interval * (1.5 ** (attempt - 1)), self.deploy_retry_interval * 4)
                        delay += random.uniform(0, 5)
                        if delay > remaining_time:
                            delay = remaining_time
                        self.logger.info(f"{search_id_prefix}Retrying deploy in {delay:.1f}s")
                        yield {'type': 'progress', 
                               'message': f'Deploy error: retrying in {delay:.1f}s (remaining: {remaining_time:.1f}s)'}
                        time.sleep(delay)
                        attempt += 1
                    else:
                        yield {'type': 'error', 
                               'message': f'Deploy failed after {time.time() - start_time:.1f}s: {str(e)}'}
                        return
            
            if not deploy_result:
                elapsed_time = time.time() - start_time
                error_msg = f"Lookup deploy failed after {elapsed_time:.1f}s timeout " \
                            f"(max: {self.deploy_max_wait_time}s)"
                self.logger.error(f"{search_id_prefix}{error_msg}")
                yield {'type': 'error', 'message': error_msg}
                return
            
            self.logger.info(f"{search_id_prefix}Deploy successful")
            
            # Create appropriate success message based on commit/deploy results
            if commit_success:
                if commit_hash == "gitignore-ignored":
                    success_details = "upload and deploy succeeded (commit blocked by gitignore)"
                else:
                    success_details = "upload, commit, and deploy all succeeded"
            else:
                success_details = "upload and deploy succeeded (commit failed)"
                
            success_msg = (
                f"Complete sync operation successful for {lookup} to {workergroup} "
                f"({actual_mode}-based lookup: {success_details})"
            )
            self.logger.info(f"{search_id_prefix}{success_msg}")
            yield {'type': 'success', 'message': success_msg}
            
        except Exception as e:
            search_id_prefix = self._get_search_id_prefix()
            error_msg = f"Sync operation failed: {str(e)}"
            self.logger.error(f"{search_id_prefix}{error_msg}")
            yield {'type': 'error', 'message': error_msg}


    def _deploy_with_extended_wait(self, cribl_cloud_url: str, workergroup: str, 
                                  commit_hash: str, progress_callback=None) -> bool:
        """
        Deploy with extended wait times for lookup processing.
        """
        search_id_prefix = self._get_search_id_prefix()
        self.logger.debug(f"{search_id_prefix}Deploying commit {commit_hash} to workergroup {workergroup}")
        
        # Brief pause to let lookup processing finish
        wait_time = 5
        self.logger.debug(f"{search_id_prefix}Waiting {wait_time}s before deploy to ensure lookup processing " \
                          f"completion")
        time.sleep(wait_time)
        
        return self.deploy_lookup(cribl_cloud_url, workergroup, commit_hash, progress_callback=progress_callback)


    def _deploy_with_extended_wait_generator(self, cribl_cloud_url: str, workergroup: str, commit_hash: str):
        """Generator-based deploy with extended wait times for lookup processing."""
        self.logger.debug(f"{self._get_search_id_prefix()}Deploying commit {commit_hash} to workergroup {workergroup}")
        
        # Brief pause to let lookup processing finish
        wait_time = 5
        self.logger.debug(f"{self._get_search_id_prefix()}Waiting {wait_time}s before deploy to ensure lookup " \
                          f"processing completion")
        yield {
            'type': 'progress', 
            'message': f'Waiting {wait_time}s before deploy to ensure lookup processing completion'
            }
        time.sleep(wait_time)
        
        # Use generator-based deploy lookup
        for deploy_progress in self.deploy_lookup_generator(cribl_cloud_url, workergroup, commit_hash):
            yield deploy_progress


    def deploy_lookup_generator(self, cribl_cloud_url: str, workergroup: str, commit_hash: str):
        """Generator-based deploy_lookup that yields progress messages."""
        try:
            self.logger.info(f"{self._get_search_id_prefix()}Deploying lookup to worker group: {workergroup} " \
                             f"with commit: {commit_hash}")
            self.logger.debug(f"{self._get_search_id_prefix()}Deploy URL: {cribl_cloud_url}")
            
            # Check if we have a valid token first
            if not self.token:
                yield {'type': 'error', 'message': 'No authentication token available for deployment'}
                return
            
            self.logger.debug(f"{self._get_search_id_prefix()}Using token for deploy: {self.token[:20]}...")
            
            # Handle gitignore scenario where commit failed but upload succeeded
            deploy_version = commit_hash
            if commit_hash == "gitignore-ignored":
                self.logger.info(f"{self._get_search_id_prefix()}Commit was blocked by gitignore, getting current " \
                                 f"effective version for deploy")
                self.logger.debug(f"{self._get_search_id_prefix()}Original commit_hash was: {commit_hash}")
                
                current_version = self.get_current_group_version(cribl_cloud_url, workergroup)
                if current_version:
                    deploy_version = current_version
                    self.logger.info(f"{self._get_search_id_prefix()}Using current effective version for deploy: " \
                                     f"{deploy_version}")
                    self.logger.debug(f"{self._get_search_id_prefix()}Successfully retrieved current version: " \
                                      f"{current_version}")
                    yield {
                        'type': 'progress', 
                        'message': f'Using current config version {deploy_version} for gitignore deployment'
                        }
                else:
                    self.logger.error(f"{self._get_search_id_prefix()}Could not determine current version for " \
                                      f"gitignore scenario")
                    self.logger.debug(f"{self._get_search_id_prefix()}get_current_group_version returned None")
                    yield {
                        'type': 'error', 
                        'message': 'Could not determine current version for gitignore deployment'
                    }
                    return
            else:
                self.logger.debug(f"{self._get_search_id_prefix()}Normal commit scenario, using commit_hash as " \
                                  f"deploy_version: {deploy_version}")
                
            # Update the commit_hash variable for the rest of the function
            commit_hash = deploy_version
            
            # Determine deployment strategy and call appropriate method
            if self.skip_mode_detection:
                self.logger.debug(f"{self._get_search_id_prefix()}Skipping mode detection, using default timeout (60s)")
                yield {'type': 'progress', 'message': 'Using default deployment timeout (60s)'}
                for default_progress in self._poll_deployment_status_generator(cribl_cloud_url, 
                                                                               workergroup, 
                                                                               commit_hash, 
                                                                               max_wait_time=60):
                    yield default_progress
                return
                
            try:
                self.logger.debug(f"{self._get_search_id_prefix()}Checking if worker group is in leader mode")
                
                # Try version check with retry for mode detection (non-critical)
                version_response = None
                for attempt in range(2):  # Try twice
                    try:
                        version_response = self._make_http_request(
                            'GET',
                            f"{cribl_cloud_url}/api/v1/version", 
                            headers={"Authorization": f"Bearer {self.token}"},
                            timeout=20,  # 20 second timeout per attempt
                            verify=self.verify_ssl
                        )
                        break  # Success, exit retry loop
                    except Exception as retry_error:
                        self.logger.debug(f"{self._get_search_id_prefix()}Version check attempt {attempt + 1} " \
                                          f"failed: {retry_error}")
                        if attempt == 1:  # Last attempt
                            raise retry_error
                
                if not version_response:
                    raise Exception("No response received from version endpoint")
                
                self.logger.debug(f"{self._get_search_id_prefix()}Version check response: " \
                                  f"{version_response.status_code}")
                self.logger.debug(f"{self._get_search_id_prefix()}Version check response text: " \
                                  f"{version_response.text[:200]}...")
                
                if version_response.status_code == 200:
                    try:
                        version_data = version_response.json()
                        self.logger.debug(f"{self._get_search_id_prefix()}Version data keys: " \
                                          f"{list(version_data.keys())}")
                        
                        # Check if we're in leader mode
                        is_leader_mode = version_data.get("distributed", {}).get("mode") == "leader"
                        self.logger.debug(f"{self._get_search_id_prefix()}Leader mode: {is_leader_mode}")
                        
                        if is_leader_mode:
                            yield {'type': 'progress', 'message': 'Deploying to leader-mode worker group (30s timeout)'}
                            for leader_progress in self._poll_deployment_status_generator(cribl_cloud_url, 
                                                                                          workergroup, 
                                                                                          commit_hash, 
                                                                                          max_wait_time=30):
                                yield leader_progress
                            return
                        else:
                            yield {'type': 'progress', 'message': 'Deploying to single-mode worker group (90s timeout)'}
                            for single_progress in self._poll_deployment_status_generator(cribl_cloud_url, 
                                                                                          workergroup, 
                                                                                          commit_hash, 
                                                                                          max_wait_time=90):
                                yield single_progress
                            return
                    except ValueError as json_error:
                        self.logger.warning(f"{self._get_search_id_prefix()}Version check returned non-JSON " \
                                            f"response: {json_error}")
                        yield {'type': 'progress', 'message': 'Version check failed, using default timeout (60s)'}
                        for fallback_progress in self._poll_deployment_status_generator(cribl_cloud_url, 
                                                                                        workergroup, 
                                                                                        commit_hash, 
                                                                                        max_wait_time=60):
                            yield fallback_progress
                        return
                        
                else:
                    yield {'type': 'progress', 'message': 'Unable to determine mode, using default timeout (60s)'}
                    for default_progress in self._poll_deployment_status_generator(cribl_cloud_url, 
                                                                                  workergroup, 
                                                                                  commit_hash, 
                                                                                  max_wait_time=60):
                        yield default_progress
                    return
                    
            except Exception as mode_error:
                self.logger.warning(f"{self._get_search_id_prefix()}Could not determine deployment mode: {mode_error}")
                yield {'type': 'progress', 'message': 'Could not determine mode, using default timeout (60s)'}
                for fallback_progress in self._poll_deployment_status_generator(cribl_cloud_url, 
                                                                                workergroup, 
                                                                                commit_hash, 
                                                                                max_wait_time=60):
                    yield fallback_progress
                return
                
        except Exception as e:
            error_msg = f"Error deploying lookup: {str(e)}"
            self.logger.error(f"{self._get_search_id_prefix()}{error_msg}")
            yield {'type': 'error', 'message': error_msg}


    def _poll_deployment_status_generator(self, cribl_cloud_url: str, workergroup: str, commit_hash: str,
                               max_wait_time: int = 60, poll_interval: int = 3):
        """Generator-based deployment status polling that yields progress messages."""
        try:
            self.logger.debug(f"{self._get_search_id_prefix()}Starting deployment polling for commit " \
                              f"{commit_hash} to {workergroup}")
            
            # Use the correct Cribl.Cloud deployment endpoint
            deploy_url = f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}/deploy"
            payload = {
                "version": commit_hash
            }
            
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }
            
            self.logger.debug(f"{self._get_search_id_prefix()}Deployment URL: {deploy_url}")
            self.logger.debug(f"{self._get_search_id_prefix()}Deployment payload: {payload}")
            
            deploy_response = self._make_http_request(
                'PATCH',
                deploy_url,
                json_data=payload,
                headers=headers,
                timeout=30,
                verify=self.verify_ssl
            )
            
            self.logger.debug(f"{self._get_search_id_prefix()} Deploy response status: {deploy_response.status_code}")
            self.logger.debug(f"{self._get_search_id_prefix()} Deploy response: {deploy_response.text}")
            
            if deploy_response.status_code not in [200, 201]:
                error_msg = f"Failed to start deployment: {deploy_response.status_code} - {deploy_response.text}"
                self.logger.error(f"{self._get_search_id_prefix()}{error_msg}")
                yield {'type': 'error', 'message': error_msg}
                return
            
            try:
                deploy_data = deploy_response.json()
                self.logger.debug(f"{self._get_search_id_prefix()} Deploy response JSON: {deploy_data}")
            except ValueError as json_error:
                self.logger.error(f"{self._get_search_id_prefix()} Deploy response is not valid JSON: " \
                                  f"{json_error}. Response: {deploy_response.text[:200]}...")
                yield {'type': 'error', 'message': error_msg}
                return
                
            # For PATCH /master/groups/{groupName}/deploy, successful response contains Worker Group info
            # with configVersion matching the deployed commit hash
            if deploy_data.get("items") and len(deploy_data["items"]) > 0:
                group_info = deploy_data["items"][0]
                deployed_version = group_info.get("configVersion")
                
                if deployed_version == commit_hash:
                    success_msg = f"Deployment successful - commit {commit_hash} deployed to {workergroup}"
                    self.logger.info(f"{self._get_search_id_prefix()}{success_msg}")
                    yield {'type': 'success', 'message': success_msg}
                    return
                else:
                    # Version mismatch - deployment may still be in progress, wait briefly and retry
                    self.logger.debug(f"{self._get_search_id_prefix()}Deployed version {deployed_version} does not " \
                                      f"match expected {commit_hash}")
                    time.sleep(2)
                    
                    # Try one more time
                    deploy_response_retry = self._make_http_request(
                        'GET',
                        f"{cribl_cloud_url}/api/v1/master/groups/{workergroup}",
                        headers=headers,
                        timeout=10,
                        verify=self.verify_ssl
                    )
                    
                    if deploy_response_retry.status_code == 200:
                        try:
                            retry_data = deploy_response_retry.json()
                            if retry_data.get("items") and len(retry_data["items"]) > 0:
                                retry_version = retry_data["items"][0].get("configVersion")
                                if retry_version == commit_hash:
                                    success_msg = f"Deployment successful - commit {commit_hash} deployed " \
                                                  f"to {workergroup}"
                                    self.logger.info(f"{self._get_search_id_prefix()}{success_msg}")
                                    yield {'type': 'success', 'message': success_msg}
                                    return
                        except ValueError:
                            pass
                    
                    error_msg = f"Deployment completed but version mismatch. Expected: {commit_hash}, " \
                                f"Got: {deployed_version}"
                    self.logger.error(f"{self._get_search_id_prefix()}{error_msg}")
                    yield {'type': 'error', 'message': error_msg}
                    return
            else:
                error_msg = f"Unexpected deploy response format: {deploy_data}"
                self.logger.error(f"{self._get_search_id_prefix()}{error_msg}")
                yield {'type': 'error', 'message': error_msg}
                    
        except Exception as e:
            error_msg = f"Error during deployment: {str(e)}"
            self.logger.error(f"{self._get_search_id_prefix()}{error_msg}")
            yield {'type': 'error', 'message': error_msg}
    
    
    def _convert_to_csv(self, data: List[Dict]) -> str:
        """
        Convert list of dictionaries to CSV string
        
        Args:
            data: List of dictionaries
            
        Returns:
            str: CSV formatted string
        """
        if not data:
            return ""
        
        output = io.StringIO()
        
        fieldnames = list(data[0].keys())
        
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        
        return output.getvalue()

