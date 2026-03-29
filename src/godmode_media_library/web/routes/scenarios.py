from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel

from ..shared import _create_task, _finish_task, _update_progress, logger

router = APIRouter()


class ScenarioCreateRequest(BaseModel):
    name: str
    description: str = ""
    icon: str = "\U0001f3ac"
    color: str = "#58a6ff"
    steps: list[dict] = []
    trigger: dict = {}


class ScenarioUpdateRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    icon: str | None = None
    color: str | None = None
    steps: list[dict] | None = None
    trigger: dict | None = None


@router.get("/scenarios")
def get_scenarios():
    """List all saved scenarios."""
    from ...scenarios import list_scenarios

    return {"scenarios": list_scenarios()}


@router.get("/scenarios/templates")
def get_scenario_templates():
    """Get built-in scenario templates."""
    from ...scenarios import get_templates

    return {"templates": get_templates()}


@router.get("/scenarios/step-types")
def get_step_types():
    """Get available step types for building scenarios."""
    from ...scenarios import STEP_TYPES

    return {"step_types": STEP_TYPES}


@router.get("/scenarios/triggers")
def check_triggers():
    """Check if any volume-mount triggers match currently mounted volumes."""
    from ...scenarios import check_volume_triggers

    return {"triggered": check_volume_triggers()}


@router.get("/scenarios/{scenario_id}")
def get_scenario_detail(scenario_id: str):
    """Get a single scenario by ID."""
    from ...scenarios import get_scenario

    sc = get_scenario(scenario_id)
    if not sc:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return sc


@router.post("/scenarios")
def create_new_scenario(body: ScenarioCreateRequest):
    """Create a new scenario."""
    from ...scenarios import create_scenario

    return create_scenario(body.model_dump())


@router.put("/scenarios/{scenario_id}")
def update_existing_scenario(scenario_id: str, body: ScenarioUpdateRequest):
    """Update a scenario."""
    from ...scenarios import update_scenario

    data = {k: v for k, v in body.model_dump().items() if v is not None}
    result = update_scenario(scenario_id, data)
    if not result:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return result


@router.delete("/scenarios/{scenario_id}")
def delete_existing_scenario(scenario_id: str):
    """Delete a scenario."""
    from ...scenarios import delete_scenario

    if not delete_scenario(scenario_id):
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return {"status": "ok"}


@router.post("/scenarios/{scenario_id}/duplicate")
def duplicate_existing_scenario(scenario_id: str):
    """Duplicate a scenario."""
    from ...scenarios import duplicate_scenario

    result = duplicate_scenario(scenario_id)
    if not result:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")
    return result


@router.post("/scenarios/{scenario_id}/run")
def run_scenario(scenario_id: str, request: Request, background_tasks: BackgroundTasks):
    """Execute a scenario (background task)."""
    from ...scenarios import execute_scenario, get_scenario

    sc = get_scenario(scenario_id)
    if not sc:
        raise HTTPException(status_code=404, detail="Scénář nenalezen")

    task = _create_task(f"scenario:{sc['name']}")
    catalog_path = str(request.app.state.catalog_path)

    def run():
        try:
            result = execute_scenario(
                scenario_id=scenario_id,
                catalog_path=catalog_path,
                progress_fn=lambda p: _update_progress(task.id, p),
            )
            _finish_task(task.id, result=result)
        except Exception as e:
            logger.exception("Scenario execution failed")
            _finish_task(task.id, error=str(e))

    background_tasks.add_task(run)
    return {"task_id": task.id, "status": "started", "scenario": sc["name"]}
