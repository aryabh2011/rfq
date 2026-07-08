import asyncio

from src.activity_feed import ActivityFeed


def test_publish_records_history_with_type_and_timestamp():
    feed = ActivityFeed()
    feed.publish("rfq_quoted", rfq_id="abc", yes_bid=0.4)
    [event] = feed.history()
    assert event["type"] == "rfq_quoted"
    assert event["rfq_id"] == "abc"
    assert event["yes_bid"] == 0.4
    assert "ts" in event


def test_history_is_bounded():
    feed = ActivityFeed(history_size=3)
    for i in range(5):
        feed.publish("rfq_skipped", rfq_id=str(i))
    history = feed.history()
    assert len(history) == 3
    assert [e["rfq_id"] for e in history] == ["2", "3", "4"]


async def test_subscriber_receives_published_events():
    feed = ActivityFeed()
    queue = feed.subscribe()
    feed.publish("rfq_quoted", rfq_id="abc")
    event = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert event["type"] == "rfq_quoted"
    assert event["rfq_id"] == "abc"


async def test_unsubscribe_stops_delivery():
    feed = ActivityFeed()
    queue = feed.subscribe()
    feed.unsubscribe(queue)
    feed.publish("rfq_quoted", rfq_id="abc")
    assert queue.empty()


def test_publish_never_raises_when_a_subscriber_queue_is_full():
    feed = ActivityFeed()
    queue = feed.subscribe()
    # fill the subscriber's queue completely, then publish once more -- must not raise
    from src.activity_feed import SUBSCRIBER_QUEUE_SIZE
    for i in range(SUBSCRIBER_QUEUE_SIZE):
        feed.publish("rfq_skipped", rfq_id=str(i))
    feed.publish("rfq_skipped", rfq_id="overflow")  # would raise QueueFull if unguarded
    assert queue.full()


def test_history_survives_a_full_subscriber_queue():
    feed = ActivityFeed(history_size=5)
    feed.subscribe()  # never drained
    from src.activity_feed import SUBSCRIBER_QUEUE_SIZE
    for i in range(SUBSCRIBER_QUEUE_SIZE + 2):
        feed.publish("rfq_skipped", rfq_id=str(i))
    assert len(feed.history()) == 5
