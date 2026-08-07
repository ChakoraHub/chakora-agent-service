# agent_service.py
import os
import json
import uuid
from datetime import datetime
import boto3
import requests
from fastapi import FastAPI
from kafka import KafkaConsumer, KafkaProducer

# CONFIG
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "Bookings")
LLAMA3_URL = os.environ.get("LLAMA3_URL", "http://localhost:11434/api/generate")

# APP
app = FastAPI(title="Agent Service")

# AWS
# Read AWS configuration from environment and create a boto3 Session.
AWS_REGION = os.environ.get("AWS_REGION", "eu-north-1")
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN")

session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION,
)

dynamodb_kwargs = {}
if AWS_ENDPOINT_URL:
    dynamodb_kwargs["endpoint_url"] = AWS_ENDPOINT_URL

dynamodb = session.resource("dynamodb", **dynamodb_kwargs)
booking_table = dynamodb.Table(DYNAMODB_TABLE)

# KAFKA

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode()
)

consumer = KafkaConsumer(
    "student.learning.updated",
    #"meeting.suggestion.requested",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode())
)

# KAFKA PUBLISH

def publish(topic, payload):
    print(f"📤 Kafka publish request → {topic} | keys={list(payload.keys())}")
    producer.send(topic, payload)
    producer.flush()
    print(f"Kafka -> {topic}: {payload}")

# DYNAMODB
def get_student_bookings(student_email):
    response = booking_table.scan()
    rows = []
    for item in response.get("Items", []):
        if item.get("student_email") == student_email:
            rows.append(item)
    rows.sort(
        key=lambda x: (
            x.get("booking_date", ""),
            x.get("start_time", ""),
            x.get("created_at", ""),
        ),
        reverse=True,
    )
    return rows

# In-memory fallback: external cache decoupled for now.
def get_student_memory(student_email):
    _ = student_email
    return []

# In-memory fallback: external cache decoupled for now.
def get_student_skills(student_email):
    _ = student_email
    return []

# SIMPLE SLOT ENGINE
def recommend_slot():
    return "18:00"

# SIMPLE PRICE ENGINE
def recommend_price(previous_booking_count):
    if previous_booking_count > 10:
        return 999
    if previous_booking_count > 5:
        return 799
    return 699

# LLM
def call_llama(student_email,
               booking_count,
               skills,
               summaries,
               recent_bookings,
               subject=None,
               booking_reason=None,
               requested_duration=None):
    rag_query = f"""
Student:{student_email}
Subject:{subject or ''}
Reason:{booking_reason or ''}
RequestedDuration:{requested_duration or ''}
"""

    prompt = f"""

Student Email:
{student_email}
Subject:
{subject or ''}
Booking Reason:
{booking_reason or ''}
Requested Duration:
{requested_duration or ''}
Previous Sessions:
{booking_count}
Skills:
{skills}
Knowledge:
{summaries}
Recent 5 Bookings:
{recent_bookings}
RAG Query Context:
{rag_query}

Recommend:

1 Topic
2 Duration
3 Complexity
4 Reasoning

Return JSON only.

"""

    payload = {
        "model": "llama3",
        "prompt": prompt,
        "stream": False
    }
    response = requests.post(
        LLAMA3_URL,
        json=payload,
        timeout=60
    )

    data = response.json()
    return data.get("response", "")

# AGENT
def generate_recommendation(
    student_email,
    subject=None,
    booking_reason=None,
    requested_duration=None,
):
    bookings = get_student_bookings(student_email)
    recent_bookings = bookings[:5]
    booking_count = len(bookings)
    summaries = get_student_memory(student_email)
    skills = get_student_skills(student_email)

    recent_booking_summary = [
        {
            "booking_date": b.get("booking_date"),
            "start_time": b.get("start_time"),
            "duration_minutes": b.get("duration_minutes"),
            "complexity": b.get("complexity"),
            "price": b.get("price"),
            "status": b.get("status"),
        }
        for b in recent_bookings
    ]

    llm_output = call_llama(
        student_email,
        booking_count,
        skills,
        summaries,
        recent_booking_summary,
        subject,
        booking_reason,
        requested_duration,
    )
    recommendation = {
        "recommendation_id":
            str(uuid.uuid4()),
        "student_email":
            student_email,
        "recent_booking_count":
            len(recent_bookings),
        "recommended_slot":
            recommend_slot(),
        "recommended_price":
            recommend_price(
                booking_count
            ),
        "llm_output":
            llm_output,
        "generated_at":
            datetime.utcnow().isoformat()
    }
    return recommendation

# CONSUMER

def consume_learning_events():
    print(
        "Listening: student.learning.updated, meeting.suggestion.requested"
    )
    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        payload = message.value
        source_topic = getattr(message, "topic", "")
        student_email = payload.get(
            "student_email"
        ) or payload.get("email")
        booking_reason = payload.get("booking_reason")
        subject = payload.get("subject")
        requested_duration = payload.get("requested_duration")
        if not student_email:
            continue
        recommendation = (
            generate_recommendation(
                student_email,
                subject=subject,
                booking_reason=booking_reason,
                requested_duration=requested_duration,
            )
        )

        # if source_topic == "meeting.suggestion.requested":
        #     publish(
        #         "meeting.suggestion.completed",
        #         {
        #             "request_id": payload.get("request_id"),
        #             "student_email": student_email,
        #             "date": payload.get("date"),
        #             "booking_reason": payload.get("booking_reason"),
        #             "recommendation": recommendation,
        #             "source": "agent_service",
        #             "published_at": datetime.utcnow().isoformat(),
        #         }
        #     )

        # publish(
        #     "agent.recommendation.created",
        #     {
        #         **recommendation,
        #         "source_topic": source_topic,
        #     }
        # )

# FASTAPI
@app.get("/health")
def health():
    return {
        "status": "UP"
    }

@app.post(
    "/agent/recommend-next-session"
)

def recommend_next_session(
    request: dict
):
    student_email = request.get(
        "student_email"
    )
    recommendation = (
        generate_recommendation(
            student_email
        )
    )
    return recommendation

# STARTUP
@app.on_event("startup")
async def startup_event():
    import threading
    t = threading.Thread(
        target=consume_learning_events,
        daemon=True
    )
    t.start()
    print(
        "Agent Service Started"
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )