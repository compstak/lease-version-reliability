import logging
from random import random
import time
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException, Request
import httpx

from server.api.schemas.token import (
    OAuth2ClientCredentials,
    OAuth2ClientCredentialsRequestForm,
    TokenResponse,
)
from server.config.settings import settings

router = APIRouter()

logger = logging.getLogger(__name__)

oauth2_scheme = OAuth2ClientCredentials(tokenUrl="token")


def retrieve_token(
    headers: Dict[str, str],
    data: Dict[str, str],
) -> TokenResponse:
    url = f"{settings.CS_AUTH_URL}/api/oauth2/token"

    token_attempt_count = 1

    response = httpx.post(url=url, headers=headers, data=data)

    while not response.status_code == httpx.codes.OK:
        if token_attempt_count <= settings.MAX_TOKEN_ATTEMPTS:
            time.sleep(pow(2, token_attempt_count) * random())
            response = httpx.post(url=url, data=data)
            token_attempt_count += 1
        else:
            raise HTTPException(status_code=400, detail=response.text)

    token: TokenResponse = response.json()

    return token


def get_cs_auth_headers() -> Dict[str, str]:
    token: TokenResponse = retrieve_token(
        headers={
            "accept": "application/json",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "client_credentials",
            "client_id": settings.CS_CLIENT_ID,
            "client_secret": settings.CS_CLIENT_SECRET,
            "scope": settings.CS_SCOPE,
        },
    )

    token_type = token["token_type"]
    access_token = token["access_token"]

    return {"Authorization": f"{token_type} {access_token}"}


@router.post("/token", tags=["token"], response_model=TokenResponse)
def cs_login(
    request: Request,
    form_data: OAuth2ClientCredentialsRequestForm = Depends(),
) -> TokenResponse:
    """
    Client Credential token endpoint
    """
    auth_header = request.headers.get("authorization")
    client_id = form_data.client_id
    client_secret = form_data.client_secret

    headers = {
        "accept": "application/json",
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "client_credentials",
        "scope": settings.CS_SCOPE,
    }

    if auth_header:
        headers["authorization"] = auth_header
        return retrieve_token(headers, data)
    elif client_id and client_secret:
        data["client_id"] = client_id
        data["client_secret"] = client_secret
        return retrieve_token(headers, data)
    else:
        raise HTTPException(status_code=400, detail="Incorrect credential")
