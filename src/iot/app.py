from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from contextlib import asynccontextmanager
import sqlite3
import asyncio
import json
from datetime import datetime

DB_NAME = "bin_data.db"

# Change these according to your actual bin
EMPTY_DISTANCE = 30.0   # cm - bin is empty
FULL_DISTANCE = 5.0     # cm - bin is full

# Notifications happen when these fill levels are reached
THRESHOLDS = [10, 30, 50]

# Keep track of which thresholds have already triggered
triggered_thresholds = set()

# Connected SSE clients
clients = []


class DistanceData(BaseModel):
    distance: float


def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            distance REAL NOT NULL,
            fill_percentage REAL NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT NOT NULL,
            fill_percentage REAL NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def calculate_fill_percentage(distance: float):
    """
    Smaller distance means the garbage is closer to the sensor,
    therefore the bin is more full.
    """

    fill = (
        (EMPTY_DISTANCE - distance)
        / (EMPTY_DISTANCE - FULL_DISTANCE)
    ) * 100

    return max(0, min(100, fill))


async def send_notification(message, fill_percentage):

    now = datetime.now().isoformat()

    # Store notification
    conn = get_db()

    conn.execute(
        """
        INSERT INTO notifications
        (message, fill_percentage, created_at)
        VALUES (?, ?, ?)
        """,
        (message, fill_percentage, now)
    )

    conn.commit()
    conn.close()

    # Send notification to all connected browsers
    data = {
        "message": message,
        "fill_percentage": fill_percentage,
        "created_at": now
    }

    dead_clients = []

    for queue in clients:
        try:
            await queue.put(data)
        except Exception:
            dead_clients.append(queue)

    for queue in dead_clients:
        if queue in clients:
            clients.remove(queue)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(lifespan=lifespan)

templates = Jinja2Templates(directory="templates")


@app.post("/distance")
async def receive_distance(data: DistanceData):

    distance = data.distance

    fill_percentage = calculate_fill_percentage(distance)

    now = datetime.now().isoformat()

    # Store reading
    conn = get_db()

    conn.execute(
        """
        INSERT INTO readings
        (distance, fill_percentage, created_at)
        VALUES (?, ?, ?)
        """,
        (distance, fill_percentage, now)
    )

    conn.commit()
    conn.close()

    # Check notification thresholds
    for threshold in THRESHOLDS:

        if fill_percentage >= threshold:

            if threshold not in triggered_thresholds:

                message = (
                    f"⚠️ Bin is {threshold}% full "
                    f"(Distance: {distance:.2f} cm)"
                )

                await send_notification(
                    message,
                    fill_percentage
                )

                triggered_thresholds.add(threshold)

        else:
            # Reset threshold when level goes below it
            triggered_thresholds.discard(threshold)

    return {
        "status": "success",
        "distance": distance,
        "fill_percentage": round(fill_percentage, 2)
    }


@app.get("/events")
async def events():

    queue = asyncio.Queue()

    clients.append(queue)

    async def event_generator():

        try:

            while True:

                data = await queue.get()

                yield f"data: {json.dumps(data)}\n\n"

        except asyncio.CancelledError:

            if queue in clients:
                clients.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):

    conn = get_db()

    readings = conn.execute(
        """
        SELECT *
        FROM readings
        ORDER BY id DESC
        LIMIT 100
        """
    ).fetchall()

    notifications = conn.execute(
        """
        SELECT *
        FROM notifications
        ORDER BY id DESC
        LIMIT 50
        """
    ).fetchall()

    conn.close()

    latest = readings[0] if readings else None

    return templates.TemplateResponse(
    request=request,
    name="index.html",
    context={
        "readings": readings,
        "notifications": notifications,
        "latest": latest,
    },
)