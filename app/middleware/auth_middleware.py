import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

PUBLIC_PATHS = ["/docs", "/redoc", "/openapi.json", "/health", "/"]


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Bypass for public endpoints
        if request.url.path in PUBLIC_PATHS:
            return await call_next(request)

        expected_token = os.getenv("JWT_TOKEN")
        # No token configured => auth disabled (handy for local development)
        if not expected_token:
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(status_code=401,
                                content={"detail": "Unauthorized - No token provided"})

        token = auth_header.split("Bearer ")[1]
        try:
            if token != expected_token:
                return JSONResponse(status_code=401,
                                    content={"detail": "Invalid token"})
        except Exception as e:
            return JSONResponse(status_code=500,
                                content={"detail": f"Server error: {str(e)}"})

        return await call_next(request)
