from fastapi import Header, HTTPException


async def require_bearer_token(
    authorization: str | None = Header(default=None),
) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail={
                "code": "UNAUTHORIZED",
                "message": "A Bearer access token is required.",
            },
        )
    return authorization.removeprefix("Bearer ").strip()
