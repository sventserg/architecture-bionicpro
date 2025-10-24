import os
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
import requests
import secrets
import hashlib
import base64
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BFF for Reports")

CONFIG = {
    "keycloak_url": os.getenv("KEYCLOAK_URL", "http://keycloak:8080"),
    "keycloak_realm": os.getenv("KEYCLOAK_REALM", "reports-realm"),
    "client_id": os.getenv("KEYCLOAK_CLIENT_ID", "reports-bff"),
    "client_secret": os.getenv("KEYCLOAK_CLIENT_SECRET", ""),
    "frontend_url": os.getenv("FRONTEND_URL", "http://localhost:3000"),
    "bff_url": os.getenv("BFF_URL", "http://localhost:8000"),
    "reports_api_url": os.getenv("REPORTS_API_URL", "http://reports-api:8081"),
    "session_max_age": int(os.getenv("SESSION_MAX_AGE", "3600"))
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=[CONFIG["frontend_url"], "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sessions: Dict[str, dict] = {}

def generate_pkce_code_verifier() -> str:
    return secrets.token_urlsafe(64)

def generate_pkce_code_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode()).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "bff"}

@app.get("/auth/status")
async def auth_status(request: Request):
    session_id = request.cookies.get("session_id")
    
    if not session_id or session_id not in sessions:
        return {"authenticated": False}
    
    session_data = sessions[session_id]
    access_token = session_data.get("access_token")
    
    if access_token:
        try:
            userinfo_response = requests.get(
                f"{CONFIG['keycloak_url']}/realms/{CONFIG['keycloak_realm']}/protocol/openid-connect/userinfo",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=5
            )
            
            if userinfo_response.status_code == 200:
                return {
                    "authenticated": True,
                    "user": session_data.get("user_info", {})
                }
        except Exception as e:
            logger.warning(f"Token validation failed: {e}")
    
    if session_id in sessions:
        del sessions[session_id]
    
    return {"authenticated": False}

@app.get("/auth/login")
async def login():
    if not CONFIG["client_secret"]:
        raise HTTPException(status_code=500, detail="Client secret not configured")
    
    state = secrets.token_urlsafe(16)
    code_verifier = generate_pkce_code_verifier()
    code_challenge = generate_pkce_code_challenge(code_verifier)
    
    sessions[state] = {
        "code_verifier": code_verifier,
        "status": "pending"
    }
    
    auth_url = (
        f"http://localhost:8090/realms/{CONFIG['keycloak_realm']}/protocol/openid-connect/auth"
        f"?client_id={CONFIG['client_id']}"
        f"&redirect_uri={CONFIG['bff_url']}/auth/callback"
        f"&response_type=code"
        f"&scope=openid profile email"
        f"&state={state}"
        f"&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
    )
    
    logger.info(f"REDIRECT TO KEYCLOAK: {auth_url}")
    return RedirectResponse(auth_url)

@app.get("/auth/callback")
def callback(code: str, state: str):
    logger.info(f"Callback received: code={code[:10]}..., state={state}")
    
    if state not in sessions:
        logger.error(f"Invalid state: {state}")
        return RedirectResponse("http://localhost:8000/auth/login")
    
    session_data = sessions[state]
    code_verifier = session_data.get("code_verifier")
    
    token_data = {
        "grant_type": "authorization_code",
        "client_id": CONFIG["client_id"],
        "client_secret": CONFIG["client_secret"],
        "code": code,
        "redirect_uri": f"{CONFIG['bff_url']}/auth/callback",
        "code_verifier": code_verifier
    }
    
    try:
        token_response = requests.post(
            f"{CONFIG['keycloak_url']}/realms/{CONFIG['keycloak_realm']}/protocol/openid-connect/token",
            data=token_data
        )
        
        logger.info(f"Token response status: {token_response.status_code}")
        
        if token_response.status_code != 200:
            logger.error(f"Token exchange failed: {token_response.text}")
            raise HTTPException(status_code=400, detail="Token exchange failed")
        
        tokens = token_response.json()
        logger.info("Token exchange successful")
        
        user_info = {}
        try:
            userinfo_response = requests.get(
                f"{CONFIG['keycloak_url']}/realms/{CONFIG['keycloak_realm']}/protocol/openid-connect/userinfo",
                headers={"Authorization": f"Bearer {tokens['access_token']}"}
            )
            
            if userinfo_response.status_code == 200:
                user_info = userinfo_response.json()
                logger.info(f"User info retrieved: {user_info.get('preferred_username')}")
            else:
                logger.warning("Could not fetch user info, using fallback")
                user_info = {
                    "preferred_username": "user_from_keycloak",
                    "email": "user@example.com", 
                    "sub": "user_id"
                }
        except Exception as userinfo_error:
            logger.warning(f"User info fetch failed: {userinfo_error}")
            user_info = {
                "preferred_username": "user_from_keycloak",
                "email": "user@example.com", 
                "sub": "user_id"
            }
        
        session_id = secrets.token_urlsafe(32)
        sessions[session_id] = {
            "access_token": tokens["access_token"],
            "refresh_token": tokens.get("refresh_token"),
            "id_token": tokens.get("id_token"),
            "user_info": user_info
        }
        
        if state in sessions:
            del sessions[state]
        
        response = RedirectResponse(CONFIG["frontend_url"])
        response.set_cookie(
            key="session_id",
            value=session_id,
            httponly=True,
            secure=False,
            samesite="lax",
            max_age=CONFIG["session_max_age"]
        )
        
        logger.info("Authentication completed successfully")
        return response
        
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Authentication failed: {str(e)}")

@app.get("/auth/user")
async def get_user(request: Request):
    session_id = request.cookies.get("session_id")
    
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    session_data = sessions[session_id]
    user_info = session_data.get("user_info", {})
    
    return user_info

@app.get("/auth/logout")
async def logout(request: Request):
    session_id = request.cookies.get("session_id")
    
    logout_success = False
    keycloak_logout_success = False
    
    if session_id and session_id in sessions:
        refresh_token = sessions[session_id].get("refresh_token")
        
        del sessions[session_id]
        logout_success = True
        
        if refresh_token:
            try:
                logout_data = {
                    "client_id": CONFIG["client_id"],
                    "client_secret": CONFIG["client_secret"],
                    "refresh_token": refresh_token
                }
                
                logout_response = requests.post(
                    f"{CONFIG['keycloak_url']}/realms/{CONFIG['keycloak_realm']}/protocol/openid-connect/logout",
                    data=logout_data,
                    timeout=5
                )
                
                if logout_response.status_code == 204:
                    logger.info("Successfully logged out from Keycloak")
                    keycloak_logout_success = True
                else:
                    logger.warning(f"Keycloak logout returned status: {logout_response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Keycloak logout failed: {str(e)}")
    
    response_data = {
        "status": "success", 
        "message": "Logged out successfully", 
        "session_cleared": logout_success,
        "keycloak_logout": keycloak_logout_success
    }
    
    logger.info(f"Logout completed: session_cleared={logout_success}, keycloak_logout={keycloak_logout_success}")
    
    response = JSONResponse(content=response_data)
    response.delete_cookie("session_id")
    
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    
    return response

@app.get("/api/reports")
def download_report(request: Request):
    session_id = request.cookies.get("session_id")
    
    if not session_id or session_id not in sessions:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    session_data = sessions[session_id]
    access_token = session_data.get("access_token")
    
    if not access_token:
        raise HTTPException(status_code=401, detail="Invalid session")
    
    try:
        api_response = requests.get(
            f"{CONFIG['reports_api_url']}/reports",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30.0
        )
        
        logger.info(f"API response status: {api_response.status_code}")
        
        if api_response.status_code != 200:
            raise HTTPException(
                status_code=api_response.status_code,
                detail="Failed to download report from API"
            )
        
        content = api_response.content
        media_type = api_response.headers.get("content-type", "application/octet-stream")
        
        from fastapi.responses import Response
        return Response(
            content=content,
            media_type=media_type,
            headers={
                "Content-Disposition": "attachment; filename=prosthesis_report.csv"
            }
        )
        
    except Exception as e:
        logger.error(f"Report download error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Report download failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")