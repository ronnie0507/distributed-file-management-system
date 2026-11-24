from fastapi import APIRouter


router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "service": "backend",
        "phase": "networking-and-api",
    }
