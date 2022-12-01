from typing import Dict, Optional

from fastapi.exceptions import HTTPException
from fastapi.openapi.models import OAuthFlows as OAuthFlowsModel
from fastapi.param_functions import Form
from fastapi.security.oauth2 import OAuth2
from fastapi.security.utils import get_authorization_scheme_param
from pydantic import BaseModel
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED

from server.config.settings import settings


class ClientCredentialsRequest(BaseModel):
    grant_type: str = "client_credentials"
    scope: str = settings.CS_SCOPE
    client_id: str = settings.CS_CLIENT_ID
    client_secret: str = settings.CS_CLIENT_SECRET


class TokenResponse(BaseModel):
    token_type: str
    access_token: str
    expires_in: int
    refresh_token: str
    scope: str


class OAuth2ClientCredentialsRequestForm:
    def __init__(
        self,
        grant_type: str = Form(
            "client_credentials",
            regex="^(client_credentials|refresh_token)$",
        ),
        scope: str = Form("admintools"),
        client_id: Optional[str] = Form(None),
        client_secret: Optional[str] = Form(None),
    ):
        self.grant_type = grant_type
        self.scopes = scope.split()
        self.client_id = client_id
        self.client_secret = client_secret


class OAuth2ClientCredentials(OAuth2):
    def __init__(
        self,
        tokenUrl: str,
        scheme_name: Optional[str] = None,
        scopes: Optional[Dict[str, str]] = None,
    ):
        if not scopes:
            scopes = {}
        flows = OAuthFlowsModel(
            clientCredentials={"tokenUrl": tokenUrl, "scopes": scopes},
        )
        super().__init__(flows=flows, scheme_name=scheme_name, auto_error=True)

    async def __call__(self, request: Request) -> Optional[str]:
        authorization: str = request.headers.get("Authorization")

        # TODO: Try combining these lines after FastAPI 0.61.2 / mypy update
        scheme_param = get_authorization_scheme_param(authorization)
        scheme: str = scheme_param[0]
        param: str = scheme_param[1]

        if not authorization or scheme.lower() != "bearer":
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Not authenticated",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return param
