from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.responses import Response
import requests
from clickhouse_driver import Client
import csv
import os
import io
import logging
from typing import Optional
from datetime import datetime as dt, timedelta
import jwt
from jwt import PyJWKClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Reports API")

CONFIG = {
    "keycloak_url": os.getenv("KEYCLOAK_URL", "http://keycloak:8080"),
    "keycloak_realm": os.getenv("KEYCLOAK_REALM", "reports-realm"), 
    "client_id": os.getenv("KEYCLOAK_CLIENT_ID", "reports-api"),
    "client_secret": os.getenv("KEYCLOAK_CLIENT_SECRET", "api-secret-key-12345")
}

clickhouse_client = Client(
    host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
    user=os.getenv("CLICKHOUSE_USER", "airflow"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "airflow"),
    database=os.getenv("CLICKHOUSE_DATABASE", "airflow")
)

jwks_client = PyJWKClient(f"{CONFIG['keycloak_url']}/realms/{CONFIG['keycloak_realm']}/protocol/openid-connect/certs")

def verify_token(authorization: Optional[str] = Header(None)):
    logger.info(f"Verifying token: {authorization[:50] if authorization else 'No token'}")
    
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")
    
    token = authorization[7:]
    
    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        
        decoded_token = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            options={"verify_exp": True, "verify_aud": False}
        )
        
        user_info = {
            "preferred_username": decoded_token.get("preferred_username"),
            "email": decoded_token.get("email", ""),
            "sub": decoded_token.get("sub", ""),
            "given_name": decoded_token.get("given_name", ""),
            "family_name": decoded_token.get("family_name", ""),
            "name": decoded_token.get("name", "")
        }
        
        if not user_info["preferred_username"]:
            raise HTTPException(status_code=401, detail="Invalid token: missing username")
        
        logger.info(f"✅ Successfully verified token for user: {user_info['preferred_username']}")
        logger.info(f"📋 Token claims: {list(decoded_token.keys())}")
        
        return {
            "token": token,
            "user_info": user_info,
            "token_info": decoded_token
        }
        
    except jwt.ExpiredSignatureError:
        logger.error("❌ Token expired")
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as e:
        logger.error(f"❌ Invalid token: {e}")
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        logger.error(f"❌ Token verification failed: {e}")
        raise HTTPException(status_code=401, detail="Token verification failed")

def get_user_mapping():
    return {
        "user1": "CLI001",
        "user2": "CLI002", 
        "prothetic1": "CLI003",
        "prothetic2": "CLI004",
        "prothetic3": "CLI005",
        "admin1": "CLI006"
    }

def get_client_id_for_user(username: str) -> str:
    user_mapping = get_user_mapping()
    
    if username in user_mapping:
        client_id = user_mapping[username]
        logger.info(f"✅ User {username} mapped to client_id: {client_id}")
        return client_id
    
    logger.warning(f"⚠️ User {username} not found in mapping, using fallback")
    
    if username.startswith('prothetic'):
        return "CLI003"
    elif username.startswith('user'):
        return "CLI001"
    else:
        return "CLI006"

@app.get("/reports")
def download_report(auth: dict = Depends(verify_token)):
    username = auth["user_info"].get("preferred_username")
    user_email = auth["user_info"].get("email")
    given_name = auth["user_info"].get("given_name", "")
    family_name = auth["user_info"].get("family_name", "")
    
    logger.info(f"=== GENERATING REPORT FOR AUTHENTICATED USER ===")
    logger.info(f"User: {username}")
    logger.info(f"Email: {user_email}")
    logger.info(f"Full Name: {given_name} {family_name}")
    
    if not username:
        raise HTTPException(status_code=400, detail="User not found")
    
    client_id = get_client_id_for_user(username)
    
    logger.info(f"🎯 Generating report for user '{username}' with client_id: {client_id}")
    
    query = """
    SELECT 
        client_id,
        date,
        avg_joint_angle,
        max_joint_angle,
        min_joint_angle, 
        avg_pressure,
        avg_battery,
        most_common_activity
    FROM user_prosthesis_reports 
    WHERE client_id = %(client_id)s
    ORDER BY date DESC
    """
    
    try:
        results = clickhouse_client.execute(
            query,
            {'client_id': client_id}
        )
        logger.info(f"📊 Found {len(results)} records in ClickHouse for client_id: {client_id}")
        
        if not results:
            logger.warning(f"⚠️ No data found for client_id {client_id}")
            raise HTTPException(
                status_code=404,
                detail={
                    "message": "No report data available for your account",
                    "details": "Please check back later or contact support if you believe this is an error",
                    "client_id": client_id,
                    "username": username
                }
            )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        raise HTTPException(
            status_code=500, 
            detail={
                "message": "Database temporarily unavailable",
                "details": "Please try again later",
                "error": str(e)
            }
        )
    
    transformed_data = []
    for row in results:
        client_id, date, avg_joint_angle, max_joint_angle, min_joint_angle, avg_pressure, avg_battery, activity = row
        
        signals = [
            ("avg_joint_angle", float(avg_joint_angle)),
            ("max_joint_angle", float(max_joint_angle)), 
            ("min_joint_angle", float(min_joint_angle)),
            ("avg_pressure", float(avg_pressure)),
            ("avg_battery", float(avg_battery))
        ]
        
        for signal_type, signal_value in signals:
            transformed_data.append({
                'user_id': client_id,
                'user_name': f"{given_name} {family_name}".strip() or username,
                'user_email': user_email or f"{username}@example.com",
                'prosthesis_type': 'arm_prosthesis',
                'signal_type': signal_type,
                'signal_value': signal_value,
                'timestamp': dt.combine(date, dt.min.time()),
                'usage_hours': 24.0,
                'battery_level': float(avg_battery),
                'created_date': date
            })
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        'User ID', 'User Name', 'Email', 'Prosthesis Type', 
        'Signal Type', 'Signal Value', 'Timestamp', 
        'Usage Hours', 'Battery Level', 'Created Date'
    ])
    
    for row in transformed_data:
        writer.writerow([
            row['user_id'],
            row['user_name'], 
            row['user_email'],
            row['prosthesis_type'],
            row['signal_type'],
            row['signal_value'],
            row['timestamp'].isoformat(),
            row['usage_hours'],
            row['battery_level'],
            row['created_date'].isoformat()
        ])
    
    csv_content = output.getvalue()
    
    logger.info(f"✅ Report generated successfully for {username} (client_id: {client_id}) - {len(transformed_data)} data points")
    
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=prosthesis_report_{username}.csv"
        }
    )

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/test/keycloak")
def test_keycloak():
    try:
        response = requests.get(f"{CONFIG['keycloak_url']}/realms/{CONFIG['keycloak_realm']}")
        return {
            "status": "success" if response.status_code == 200 else "failed",
            "keycloak_status": response.status_code,
            "message": "Keycloak is reachable" if response.status_code == 200 else "Keycloak not reachable"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Cannot connect to Keycloak: {str(e)}"
        }

@app.get("/test/jwks")
def test_jwks():
    try:
        jwks_url = f"{CONFIG['keycloak_url']}/realms/{CONFIG['keycloak_realm']}/protocol/openid-connect/certs"
        response = requests.get(jwks_url)
        return {
            "status": "success" if response.status_code == 200 else "failed",
            "jwks_status": response.status_code,
            "jwks_keys": len(response.json().get('keys', [])) if response.status_code == 200 else 0
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Cannot connect to JWKS endpoint: {str(e)}"
        }

@app.get("/test/token-info")
def test_token_info(auth: dict = Depends(verify_token)):
    return {
        "authenticated_user": auth["user_info"],
        "token_claims": list(auth["token_info"].keys()),
        "mapped_client_id": get_client_id_for_user(auth["user_info"]["preferred_username"])
    }

@app.get("/test/current-user")
def test_current_user(auth: dict = Depends(verify_token)):
    return {
        "authenticated_user": auth["user_info"],
        "mapped_client_id": get_client_id_for_user(auth["user_info"]["preferred_username"])
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081, log_level="info")