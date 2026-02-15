"""Piccolo app registration."""

from piccolo.conf.apps import AppConfig

from db.tables import (
    Campaign,
    CampaignLead,
    DailySendLog,
    Deal,
    EmailSent,
    Lead,
    Mailbox,
    SchemaVersion,
    SequenceStep,
    Session,
    TrackingEvent,
    User,
)

APP_CONFIG = AppConfig(
    app_name="coldpipe",
    migrations_folder_path="",
    table_classes=[
        Lead,
        Mailbox,
        Campaign,
        SequenceStep,
        CampaignLead,
        EmailSent,
        Deal,
        TrackingEvent,
        DailySendLog,
        SchemaVersion,
        User,
        Session,
    ],
)
