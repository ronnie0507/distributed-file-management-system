from fastapi import APIRouter


router = APIRouter(prefix="/api/nodes", tags=["nodes"])


@router.get("/health")
async def node_health() -> dict:
    return {
        "metadata_nodes": [],
        "storage_nodes": [],
        "phase": "networking-placeholder",
    }
