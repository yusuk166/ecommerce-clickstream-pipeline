"""토픽 생성 스크립트.

user-events      : 메인 이벤트 스트림 (partition 3, retention 7일)
user-events-dlq  : 처리 실패 메시지 격리용 (partition 1, retention 30일)
"""
import argparse

from confluent_kafka.admin import AdminClient, NewTopic

DAY_MS = 24 * 60 * 60 * 1000

TOPICS = [
    NewTopic(
        "user-events",
        num_partitions=3,
        replication_factor=1,
        config={"retention.ms": str(7 * DAY_MS)},
    ),
    NewTopic(
        "user-events-dlq",
        num_partitions=1,
        replication_factor=1,
        config={"retention.ms": str(30 * DAY_MS)},
    ),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)

    todo = [t for t in TOPICS if t.topic not in existing]

    if todo:
        for topic, fut in admin.create_topics(todo).items():
            try:
                fut.result()
                print(f"[ok]   {topic} 생성")
            except Exception as e:  # noqa: BLE001
                print(f"[fail] {topic}: {e}")

    # describe
    md = admin.list_topics(timeout=10)
    for t in TOPICS:
        tm = md.topics.get(t.topic)
        if tm:
            print(f"  {t.topic}: partitions={len(tm.partitions)}")


if __name__ == "__main__":
    main()
