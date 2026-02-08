"""
Generate Sample Event/Log Data
This script creates realistic user activity events in JSON format
"""

import json
import random
from datetime import datetime, timedelta

# Event types and their typical metadata
EVENT_TYPES = {
    "page_view": ["page", "referrer", "device", "browser"],
    "button_click": ["button_id", "page", "device"],
    "form_submit": ["form_id", "page", "success"],
    "video_play": ["video_id", "duration_seconds", "device"],
    "add_to_cart": ["product_id", "quantity", "price"],
    "checkout": ["cart_total", "payment_method", "items_count"],
    "search": ["query", "results_count", "filters"],
    "login": ["method", "success", "device"],
    "logout": ["session_duration_seconds", "device"],
    "error": ["error_type", "error_message", "page"],
}

PAGES = [
    "/", "/products", "/products/electronics", "/products/furniture",
    "/cart", "/checkout", "/account", "/search", "/help", "/contact",
    "/products/prod001", "/products/prod002", "/products/prod003"
]

DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
REFERRERS = ["google", "facebook", "twitter", "direct", "email", "instagram"]

def generate_user_ids(count=1000):
    """Generate user IDs (mix of registered and anonymous)"""
    users = []
    
    # 70% registered users
    for i in range(1, int(count * 0.7)):
        users.append(f"USER{str(i).zfill(5)}")
    
    # 30% anonymous users
    for i in range(int(count * 0.3)):
        users.append(f"ANON{str(random.randint(10000, 99999))}")
    
    return users

def generate_session_id():
    """Generate session ID"""
    return f"SESS{random.randint(100000, 999999)}"

def generate_metadata(event_type):
    """Generate event-specific metadata"""
    metadata = {}
    
    if event_type == "page_view":
        metadata = {
            "page": random.choice(PAGES),
            "referrer": random.choice(REFERRERS),
            "device": random.choice(DEVICES),
            "browser": random.choice(BROWSERS),
            "load_time_ms": random.randint(200, 2000),
        }
    
    elif event_type == "button_click":
        metadata = {
            "button_id": random.choice(["add_to_cart", "buy_now", "learn_more", "subscribe", "submit"]),
            "page": random.choice(PAGES),
            "device": random.choice(DEVICES),
            "position_x": random.randint(0, 1920),
            "position_y": random.randint(0, 1080),
        }
    
    elif event_type == "form_submit":
        metadata = {
            "form_id": random.choice(["newsletter", "contact", "checkout", "registration"]),
            "page": random.choice(PAGES),
            "success": random.choice([True, True, True, False]),  # 75% success
            "fields_count": random.randint(3, 10),
        }
    
    elif event_type == "video_play":
        metadata = {
            "video_id": f"VID{random.randint(100, 999)}",
            "duration_seconds": random.randint(30, 600),
            "device": random.choice(DEVICES),
            "quality": random.choice(["720p", "1080p", "4K"]),
        }
    
    elif event_type == "add_to_cart":
        metadata = {
            "product_id": f"PROD{str(random.randint(1, 15)).zfill(3)}",
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(10, 500), 2),
            "device": random.choice(DEVICES),
        }
    
    elif event_type == "checkout":
        metadata = {
            "cart_total": round(random.uniform(50, 2000), 2),
            "payment_method": random.choice(["credit_card", "paypal", "debit_card"]),
            "items_count": random.randint(1, 10),
            "shipping_method": random.choice(["standard", "express", "overnight"]),
        }
    
    elif event_type == "search":
        queries = ["laptop", "chair", "desk", "monitor", "keyboard", "mouse", "headphones"]
        metadata = {
            "query": random.choice(queries),
            "results_count": random.randint(0, 150),
            "filters": random.choice([[], ["price_low_high"], ["rating"], ["brand"]]),
            "device": random.choice(DEVICES),
        }
    
    elif event_type == "login":
        metadata = {
            "method": random.choice(["email", "google", "facebook", "apple"]),
            "success": random.choice([True, True, True, False]),
            "device": random.choice(DEVICES),
            "location": random.choice(["US", "UK", "CA", "AU", "DE"]),
        }
    
    elif event_type == "logout":
        metadata = {
            "session_duration_seconds": random.randint(60, 7200),
            "device": random.choice(DEVICES),
            "pages_viewed": random.randint(1, 50),
        }
    
    elif event_type == "error":
        metadata = {
            "error_type": random.choice(["404", "500", "timeout", "validation"]),
            "error_message": random.choice([
                "Page not found",
                "Server error",
                "Request timeout",
                "Invalid input"
            ]),
            "page": random.choice(PAGES),
            "device": random.choice(DEVICES),
        }
    
    return metadata

def generate_events(num_events=50000, start_date="2024-01-01", end_date="2024-02-08"):
    """Generate user activity events"""
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    users = generate_user_ids(1000)
    events = []
    
    # Create sessions (group of events from same user)
    num_sessions = num_events // 10  # Average 10 events per session
    
    for session in range(num_sessions):
        user_id = random.choice(users)
        session_id = generate_session_id()
        
        # Session start time
        days_diff = (end - start).days
        session_start = start + timedelta(
            days=random.randint(0, days_diff),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        # Generate 5-15 events per session
        events_in_session = random.randint(5, 15)
        current_time = session_start
        
        for i in range(events_in_session):
            event_id = f"EVT{len(events) + 1:08d}"
            
            # Choose event type (page_view most common)
            event_type = random.choices(
                list(EVENT_TYPES.keys()),
                weights=[40, 15, 8, 5, 12, 3, 8, 5, 2, 2]  # page_view most common
            )[0]
            
            # Generate metadata
            metadata = generate_metadata(event_type)
            
            # Add some events without user_id (bot traffic to filter)
            if random.random() < 0.02:  # 2% bot traffic
                user_id = f"BOT{random.randint(1000, 9999)}"
            
            event = {
                "event_id": event_id,
                "user_id": user_id,
                "event_type": event_type,
                "timestamp": current_time.isoformat() + "Z",
                "session_id": session_id,
                "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "metadata": metadata
            }
            
            events.append(event)
            
            # Next event in session (5 seconds to 5 minutes later)
            current_time += timedelta(seconds=random.randint(5, 300))
    
    return events

def add_data_quality_issues(events):
    """Introduce data quality issues for testing"""
    
    if len(events) > 1000:
        # Missing event_id
        events[100]["event_id"] = None
        
        # Missing user_id
        events[200]["user_id"] = ""
        
        # Missing timestamp
        events[300]["timestamp"] = None
        
        # Malformed JSON metadata
        events[400]["metadata"] = "not_a_dict"
        
        # Duplicate event_id
        events.append(events[500].copy())
        events.append(events[600].copy())
    
    return events

def save_to_json(events, filename):
    """Save events to JSON file (one event per line - newline-delimited JSON)"""
    with open(filename, 'w', encoding='utf-8') as f:
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    print(f"✓ Generated {filename} with {len(events)} events")

def save_sample_events(events, filename, count=100):
    """Save a small sample as pretty-printed JSON for documentation"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(events[:count], f, indent=2)
    
    print(f"✓ Generated {filename} with {count} sample events (pretty-printed)")

if __name__ == "__main__":
    print("Generating sample event data...")
    
    # Generate events
    events = generate_events(num_events=50000)
    events = add_data_quality_issues(events)
    
    # Save full dataset (newline-delimited JSON)
    save_to_json(events, "user_events.json")
    
    # Save sample for documentation
    save_sample_events(events, "user_events_sample.json", count=50)
    
    print("\n✓ Sample event data generation complete!")
    print(f"  - {len(events)} user events")
    print(f"  - Event types: {', '.join(EVENT_TYPES.keys())}")
    print(f"  - Includes bot traffic and data quality issues")