"""
OWASP security headers implementation for the 254Carbon Data Processing Pipeline.
Implements comprehensive security headers based on OWASP guidelines.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class OWASPSecurityHeaders:
    """Implements OWASP security headers for web applications."""
    
    def __init__(self, 
                 hsts_max_age: int = 31536000,  # 1 year
                 csp_report_only: bool = False,
                 frame_ancestors: List[str] = None,
                 script_sources: List[str] = None,
                 style_sources: List[str] = None,
                 img_sources: List[str] = None,
                 connect_sources: List[str] = None):
        
        self.hsts_max_age = hsts_max_age
        self.csp_report_only = csp_report_only
        self.frame_ancestors = frame_ancestors or ["'self'"]
        self.script_sources = script_sources or ["'self'", "'unsafe-inline'"]
        self.style_sources = style_sources or ["'self'", "'unsafe-inline'"]
        self.img_sources = img_sources or ["'self'", "data:", "https:"]
        self.connect_sources = connect_sources or ["'self'"]
    
    def get_all_headers(self) -> Dict[str, str]:
        """Get all OWASP security headers."""
        headers = {}
        
        # HTTP Strict Transport Security (HSTS)
        headers.update(self.get_hsts_headers())
        
        # Content Security Policy (CSP)
        headers.update(self.get_csp_headers())
        
        # X-Frame-Options
        headers.update(self.get_frame_options_headers())
        
        # X-Content-Type-Options
        headers.update(self.get_content_type_options_headers())
        
        # X-XSS-Protection
        headers.update(self.get_xss_protection_headers())
        
        # Referrer Policy
        headers.update(self.get_referrer_policy_headers())
        
        # Permissions Policy
        headers.update(self.get_permissions_policy_headers())
        
        # Cross-Origin policies
        headers.update(self.get_cross_origin_headers())
        
        # Additional security headers
        headers.update(self.get_additional_security_headers())
        
        return headers
    
    def get_hsts_headers(self) -> Dict[str, str]:
        """Get HTTP Strict Transport Security headers."""
        hsts_value = f"max-age={self.hsts_max_age}; includeSubDomains; preload"
        return {
            'Strict-Transport-Security': hsts_value
        }
    
    def get_csp_headers(self) -> Dict[str, str]:
        """Get Content Security Policy headers."""
        csp_directives = [
            f"default-src 'self'",
            f"script-src {' '.join(self.script_sources)}",
            f"style-src {' '.join(self.style_sources)}",
            f"img-src {' '.join(self.img_sources)}",
            f"connect-src {' '.join(self.connect_sources)}",
            f"font-src 'self' data:",
            f"object-src 'none'",
            f"media-src 'self'",
            f"frame-src 'none'",
            f"frame-ancestors {' '.join(self.frame_ancestors)}",
            f"form-action 'self'",
            f"base-uri 'self'",
            f"manifest-src 'self'",
            f"worker-src 'self'",
            f"child-src 'self'",
            f"upgrade-insecure-requests",
            f"block-all-mixed-content"
        ]
        
        csp_value = "; ".join(csp_directives)
        
        if self.csp_report_only:
            return {
                'Content-Security-Policy-Report-Only': csp_value
            }
        else:
            return {
                'Content-Security-Policy': csp_value
            }
    
    def get_frame_options_headers(self) -> Dict[str, str]:
        """Get X-Frame-Options headers."""
        return {
            'X-Frame-Options': 'DENY'
        }
    
    def get_content_type_options_headers(self) -> Dict[str, str]:
        """Get X-Content-Type-Options headers."""
        return {
            'X-Content-Type-Options': 'nosniff'
        }
    
    def get_xss_protection_headers(self) -> Dict[str, str]:
        """Get X-XSS-Protection headers."""
        return {
            'X-XSS-Protection': '1; mode=block'
        }
    
    def get_referrer_policy_headers(self) -> Dict[str, str]:
        """Get Referrer-Policy headers."""
        return {
            'Referrer-Policy': 'strict-origin-when-cross-origin'
        }
    
    def get_permissions_policy_headers(self) -> Dict[str, str]:
        """Get Permissions-Policy headers."""
        permissions = [
            'geolocation=()',
            'microphone=()',
            'camera=()',
            'usb=()',
            'magnetometer=()',
            'gyroscope=()',
            'accelerometer=()',
            'ambient-light-sensor=()',
            'autoplay=()',
            'encrypted-media=()',
            'fullscreen=(self)',
            'payment=()',
            'picture-in-picture=()',
            'sync-xhr=()',
            'web-share=()'
        ]
        
        return {
            'Permissions-Policy': ', '.join(permissions)
        }
    
    def get_cross_origin_headers(self) -> Dict[str, str]:
        """Get Cross-Origin headers."""
        return {
            'Cross-Origin-Embedder-Policy': 'require-corp',
            'Cross-Origin-Opener-Policy': 'same-origin',
            'Cross-Origin-Resource-Policy': 'same-origin'
        }
    
    def get_additional_security_headers(self) -> Dict[str, str]:
        """Get additional security headers."""
        return {
            'X-Permitted-Cross-Domain-Policies': 'none',
            'X-Download-Options': 'noopen',
            'X-DNS-Prefetch-Control': 'off',
            'Expect-CT': 'max-age=86400, enforce',
            'Feature-Policy': 'geolocation \'none\'; microphone \'none\'; camera \'none\'',
            'Server': '254Carbon-Data-Processing',
            'X-Powered-By': '',  # Remove X-Powered-By header
            'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0'
        }
    
    def get_api_security_headers(self) -> Dict[str, str]:
        """Get security headers specifically for API endpoints."""
        api_headers = self.get_all_headers()
        
        # Override CSP for API endpoints
        api_headers['Content-Security-Policy'] = "default-src 'none'"
        
        # Add API-specific headers
        api_headers.update({
            'X-API-Version': '1.0',
            'X-Rate-Limit': '1000',
            'X-Rate-Limit-Remaining': '999',
            'X-Rate-Limit-Reset': str(int((datetime.utcnow() + timedelta(hours=1)).timestamp())),
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Max-Age': '86400'
        })
        
        return api_headers
    
    def get_health_check_headers(self) -> Dict[str, str]:
        """Get security headers for health check endpoints."""
        health_headers = {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block'
        }
        
        return health_headers
    
    def get_metrics_headers(self) -> Dict[str, str]:
        """Get security headers for metrics endpoints."""
        metrics_headers = {
            'Content-Type': 'text/plain; version=0.0.4; charset=utf-8',
            'Cache-Control': 'no-cache, no-store, must-revalidate',
            'Pragma': 'no-cache',
            'Expires': '0',
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        }
        
        return metrics_headers


class SecurityHeaderMiddleware:
    """Middleware for applying security headers to HTTP responses."""
    
    def __init__(self, 
                 app,
                 enable_hsts: bool = True,
                 enable_csp: bool = True,
                 enable_cors: bool = True,
                 custom_headers: Optional[Dict[str, str]] = None):
        
        self.app = app
        self.enable_hsts = enable_hsts
        self.enable_csp = enable_csp
        self.enable_cors = enable_cors
        self.custom_headers = custom_headers or {}
        
        # Initialize OWASP security headers
        self.owasp_headers = OWASPSecurityHeaders()
    
    def __call__(self, environ, start_response):
        """Apply security headers to the response."""
        
        def new_start_response(status, response_headers, exc_info=None):
            # Get security headers based on the request path
            security_headers = self._get_headers_for_path(environ.get('PATH_INFO', ''))
            
            # Add custom headers
            security_headers.update(self.custom_headers)
            
            # Add security headers to response
            for header, value in security_headers.items():
                if value:  # Only add non-empty values
                    response_headers.append((header, value))
            
            return start_response(status, response_headers, exc_info)
        
        return self.app(environ, new_start_response)
    
    def _get_headers_for_path(self, path: str) -> Dict[str, str]:
        """Get appropriate security headers based on the request path."""
        
        # Health check endpoints
        if path.startswith('/health'):
            return self.owasp_headers.get_health_check_headers()
        
        # Metrics endpoints
        elif path.startswith('/metrics'):
            return self.owasp_headers.get_metrics_headers()
        
        # API endpoints
        elif path.startswith('/api/'):
            return self.owasp_headers.get_api_security_headers()
        
        # Default headers for all other endpoints
        else:
            return self.owasp_headers.get_all_headers()


class FastAPISecurityHeaders:
    """FastAPI-specific security headers implementation."""
    
    def __init__(self, 
                 enable_hsts: bool = True,
                 enable_csp: bool = True,
                 enable_cors: bool = True):
        
        self.enable_hsts = enable_hsts
        self.enable_csp = enable_csp
        self.enable_cors = enable_cors
        
        # Initialize OWASP security headers
        self.owasp_headers = OWASPSecurityHeaders()
    
    def apply_headers(self, response, path: str = ""):
        """Apply security headers to a FastAPI response."""
        
        # Get appropriate headers for the path
        if path.startswith('/health'):
            headers = self.owasp_headers.get_health_check_headers()
        elif path.startswith('/metrics'):
            headers = self.owasp_headers.get_metrics_headers()
        elif path.startswith('/api/'):
            headers = self.owasp_headers.get_api_security_headers()
        else:
            headers = self.owasp_headers.get_all_headers()
        
        # Apply headers to response
        for header, value in headers.items():
            if value:  # Only add non-empty values
                response.headers[header] = value
        
        return response
    
    def get_cors_headers(self) -> Dict[str, str]:
        """Get CORS-specific headers."""
        return {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
            'Access-Control-Max-Age': '86400',
            'Access-Control-Allow-Credentials': 'true'
        }


class SecurityHeaderValidator:
    """Validates security headers for compliance."""
    
    def __init__(self):
        self.required_headers = [
            'Strict-Transport-Security',
            'Content-Security-Policy',
            'X-Frame-Options',
            'X-Content-Type-Options',
            'X-XSS-Protection',
            'Referrer-Policy'
        ]
        
        self.recommended_headers = [
            'Permissions-Policy',
            'Cross-Origin-Embedder-Policy',
            'Cross-Origin-Opener-Policy',
            'Cross-Origin-Resource-Policy',
            'Expect-CT'
        ]
    
    def validate_headers(self, headers: Dict[str, str]) -> Dict[str, Any]:
        """Validate security headers for compliance."""
        validation_result = {
            'is_compliant': True,
            'missing_required': [],
            'missing_recommended': [],
            'warnings': [],
            'score': 0
        }
        
        # Check required headers
        for header in self.required_headers:
            if header not in headers or not headers[header]:
                validation_result['missing_required'].append(header)
                validation_result['is_compliant'] = False
        
        # Check recommended headers
        for header in self.recommended_headers:
            if header not in headers or not headers[header]:
                validation_result['missing_recommended'].append(header)
        
        # Calculate compliance score
        total_headers = len(self.required_headers) + len(self.recommended_headers)
        present_headers = total_headers - len(validation_result['missing_required']) - len(validation_result['missing_recommended'])
        validation_result['score'] = (present_headers / total_headers) * 100
        
        # Add warnings for common issues
        if 'Content-Security-Policy' in headers:
            csp = headers['Content-Security-Policy']
            if "'unsafe-inline'" in csp:
                validation_result['warnings'].append("CSP contains 'unsafe-inline' - consider removing for better security")
            if "'unsafe-eval'" in csp:
                validation_result['warnings'].append("CSP contains 'unsafe-eval' - consider removing for better security")
        
        if 'Strict-Transport-Security' in headers:
            hsts = headers['Strict-Transport-Security']
            if 'preload' not in hsts:
                validation_result['warnings'].append("HSTS missing 'preload' directive - consider adding for better security")
        
        return validation_result
    
    def get_compliance_report(self, headers: Dict[str, str]) -> str:
        """Generate a compliance report for security headers."""
        validation = self.validate_headers(headers)
        
        report = f"""
Security Headers Compliance Report
=================================

Overall Compliance: {'✅ COMPLIANT' if validation['is_compliant'] else '❌ NON-COMPLIANT'}
Compliance Score: {validation['score']:.1f}%

Required Headers Missing: {len(validation['missing_required'])}
{chr(10).join(f'  - {header}' for header in validation['missing_required'])}

Recommended Headers Missing: {len(validation['missing_recommended'])}
{chr(10).join(f'  - {header}' for header in validation['missing_recommended'])}

Warnings: {len(validation['warnings'])}
{chr(10).join(f'  - {warning}' for warning in validation['warnings'])}

Recommendations:
1. Add all missing required headers
2. Consider adding recommended headers
3. Address any warnings listed above
4. Regularly test security headers compliance
        """
        
        return report.strip()


# Global security headers instance
_security_headers = None

def get_security_headers() -> OWASPSecurityHeaders:
    """Get the global security headers instance."""
    global _security_headers
    if _security_headers is None:
        _security_headers = OWASPSecurityHeaders()
    return _security_headers

def get_fastapi_security_headers() -> FastAPISecurityHeaders:
    """Get a FastAPI security headers instance."""
    return FastAPISecurityHeaders()

def get_security_header_validator() -> SecurityHeaderValidator:
    """Get a security header validator instance."""
    return SecurityHeaderValidator()


# Example usage and testing
if __name__ == "__main__":
    # Test OWASP security headers
    print("Testing OWASP Security Headers...")
    
    owasp_headers = OWASPSecurityHeaders()
    
    # Test all headers
    all_headers = owasp_headers.get_all_headers()
    print(f"Total security headers: {len(all_headers)}")
    
    # Test API headers
    api_headers = owasp_headers.get_api_security_headers()
    print(f"API security headers: {len(api_headers)}")
    
    # Test health check headers
    health_headers = owasp_headers.get_health_check_headers()
    print(f"Health check headers: {len(health_headers)}")
    
    # Test metrics headers
    metrics_headers = owasp_headers.get_metrics_headers()
    print(f"Metrics headers: {len(metrics_headers)}")
    
    # Test validation
    validator = SecurityHeaderValidator()
    validation_result = validator.validate_headers(all_headers)
    
    print(f"\nValidation Results:")
    print(f"Compliance Score: {validation_result['score']:.1f}%")
    print(f"Missing Required: {len(validation_result['missing_required'])}")
    print(f"Missing Recommended: {len(validation_result['missing_recommended'])}")
    print(f"Warnings: {len(validation_result['warnings'])}")
    
    # Generate compliance report
    report = validator.get_compliance_report(all_headers)
    print(f"\nCompliance Report:\n{report}")
    
    print("\nOWASP Security Headers test completed successfully!")
