from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

if __package__ == "backend.api.routes":
    from ...database.session import get_db
else:
    from database.session import get_db


router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
@router.get("/health/live")
async def health() -> dict:
    return {
        "status": "ok",
        "service": "backend",
    }


@router.get("/health/ready")
def readiness(db: Session = Depends(get_db)) -> dict:
    try:
        db.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "NOT_READY",
                "message": "The service cannot currently access its metadata database.",
            },
        ) from exc
    return {"status": "ready", "service": "backend"}
