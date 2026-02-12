-- Dentists DB Schema v1
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT UNIQUE,
    first_name      TEXT    NOT NULL DEFAULT '',
    last_name       TEXT    NOT NULL DEFAULT '',
    company         TEXT    NOT NULL DEFAULT '',
    job_title       TEXT    NOT NULL DEFAULT 'Dentist',
    website         TEXT    NOT NULL DEFAULT '',
    phone           TEXT    NOT NULL DEFAULT '',
    address         TEXT    NOT NULL DEFAULT '',
    city            TEXT    NOT NULL DEFAULT '',
    state           TEXT    NOT NULL DEFAULT '',
    zip             TEXT    NOT NULL DEFAULT '',
    source          TEXT    NOT NULL DEFAULT '',
    source_url      TEXT    NOT NULL DEFAULT '',
    email_status    TEXT    NOT NULL DEFAULT 'unknown'
                    CHECK (email_status IN ('unknown','valid','invalid','catch_all','risky','missing')),
    enriched_at     TEXT,
    validated_at    TEXT,
    tags            TEXT    NOT NULL DEFAULT '',
    notes           TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_company_name
    ON leads(company, first_name, last_name) WHERE email IS NULL;
CREATE INDEX IF NOT EXISTS idx_leads_email_status ON leads(email_status);
CREATE INDEX IF NOT EXISTS idx_leads_source       ON leads(source);
CREATE INDEX IF NOT EXISTS idx_leads_city_state   ON leads(city, state);

CREATE TRIGGER IF NOT EXISTS trg_leads_updated_at
AFTER UPDATE ON leads
FOR EACH ROW
BEGIN
    UPDATE leads SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = OLD.id;
END;

-- Mailboxes for sending
CREATE TABLE IF NOT EXISTS mailboxes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT    NOT NULL UNIQUE,
    smtp_host       TEXT    NOT NULL,
    smtp_port       INTEGER NOT NULL DEFAULT 587,
    smtp_user       TEXT    NOT NULL,
    smtp_pass       TEXT    NOT NULL,
    imap_host       TEXT    NOT NULL DEFAULT '',
    imap_port       INTEGER NOT NULL DEFAULT 993,
    imap_user       TEXT    NOT NULL DEFAULT '',
    imap_pass       TEXT    NOT NULL DEFAULT '',
    daily_limit     INTEGER NOT NULL DEFAULT 30,
    warmup_day      INTEGER NOT NULL DEFAULT 0,
    is_active       INTEGER NOT NULL DEFAULT 1,
    display_name    TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

-- Campaigns
CREATE TABLE IF NOT EXISTS campaigns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'draft'
                    CHECK (status IN ('draft','active','paused','completed','archived')),
    mailbox_id      INTEGER REFERENCES mailboxes(id),
    daily_limit     INTEGER NOT NULL DEFAULT 30,
    timezone        TEXT    NOT NULL DEFAULT 'America/New_York',
    send_window_start TEXT  NOT NULL DEFAULT '08:00',
    send_window_end   TEXT  NOT NULL DEFAULT '17:00',
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns(status);

CREATE TRIGGER IF NOT EXISTS trg_campaigns_updated_at
AFTER UPDATE ON campaigns
FOR EACH ROW
BEGIN
    UPDATE campaigns SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = OLD.id;
END;

-- Sequence steps within a campaign
CREATE TABLE IF NOT EXISTS sequence_steps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id     INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    step_number     INTEGER NOT NULL,
    template_name   TEXT    NOT NULL DEFAULT '',
    subject         TEXT    NOT NULL DEFAULT '',
    delay_days      INTEGER NOT NULL DEFAULT 0,
    is_reply        INTEGER NOT NULL DEFAULT 0,
    UNIQUE(campaign_id, step_number)
);

CREATE INDEX IF NOT EXISTS idx_steps_campaign ON sequence_steps(campaign_id);

-- Many-to-many: campaign <-> leads
CREATE TABLE IF NOT EXISTS campaign_leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id     INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    lead_id         INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    current_step    INTEGER NOT NULL DEFAULT 0,
    status          TEXT    NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active','replied','bounced','unsubscribed','completed','paused')),
    enrolled_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    last_sent_at    TEXT,
    next_send_at    TEXT,
    UNIQUE(campaign_id, lead_id)
);

CREATE INDEX IF NOT EXISTS idx_cl_campaign  ON campaign_leads(campaign_id);
CREATE INDEX IF NOT EXISTS idx_cl_lead      ON campaign_leads(lead_id);
CREATE INDEX IF NOT EXISTS idx_cl_next_send ON campaign_leads(next_send_at)
    WHERE status = 'active';

-- Emails sent log
CREATE TABLE IF NOT EXISTS emails_sent (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_lead_id INTEGER REFERENCES campaign_leads(id),
    campaign_id     INTEGER REFERENCES campaigns(id),
    lead_id         INTEGER NOT NULL REFERENCES leads(id),
    mailbox_id      INTEGER REFERENCES mailboxes(id),
    step_number     INTEGER NOT NULL DEFAULT 0,
    message_id      TEXT    NOT NULL DEFAULT '',
    subject         TEXT    NOT NULL DEFAULT '',
    to_email        TEXT    NOT NULL DEFAULT '',
    from_email      TEXT    NOT NULL DEFAULT '',
    body_text       TEXT    NOT NULL DEFAULT '',
    status          TEXT    NOT NULL DEFAULT 'sent'
                    CHECK (status IN ('sent','delivered','replied','bounced','failed')),
    sent_at         TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    replied_at      TEXT,
    bounced_at      TEXT,
    bounce_reason   TEXT
);

CREATE INDEX IF NOT EXISTS idx_es_campaign  ON emails_sent(campaign_id);
CREATE INDEX IF NOT EXISTS idx_es_lead      ON emails_sent(lead_id);
CREATE INDEX IF NOT EXISTS idx_es_status    ON emails_sent(status);
CREATE INDEX IF NOT EXISTS idx_es_sent_at   ON emails_sent(sent_at);
CREATE INDEX IF NOT EXISTS idx_es_msg_id    ON emails_sent(message_id);

-- Deals pipeline
CREATE TABLE IF NOT EXISTS deals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         INTEGER NOT NULL REFERENCES leads(id),
    campaign_id     INTEGER REFERENCES campaigns(id),
    stage           TEXT    NOT NULL DEFAULT 'lead'
                    CHECK (stage IN ('lead','contacted','replied','interested',
                                     'meeting_booked','proposal_sent','closed_won','closed_lost')),
    value           REAL    NOT NULL DEFAULT 0.0,
    close_date      TEXT,
    loss_reason     TEXT,
    notes           TEXT    NOT NULL DEFAULT '',
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
    updated_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_deals_lead   ON deals(lead_id);
CREATE INDEX IF NOT EXISTS idx_deals_stage  ON deals(stage);

CREATE TRIGGER IF NOT EXISTS trg_deals_updated_at
AFTER UPDATE ON deals
FOR EACH ROW
BEGIN
    UPDATE deals SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = OLD.id;
END;

-- Tracking events (reply, bounce, unsubscribe — no open tracking)
CREATE TABLE IF NOT EXISTS tracking_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email_sent_id   INTEGER NOT NULL REFERENCES emails_sent(id),
    event_type      TEXT    NOT NULL CHECK (event_type IN ('reply','bounce','unsubscribe')),
    metadata        TEXT,
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_te_email ON tracking_events(email_sent_id);

-- Daily send log for rate limiting
CREATE TABLE IF NOT EXISTS daily_send_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    mailbox_id      INTEGER NOT NULL REFERENCES mailboxes(id),
    send_date       TEXT    NOT NULL DEFAULT (date('now')),
    count           INTEGER NOT NULL DEFAULT 0,
    UNIQUE(mailbox_id, send_date)
);

CREATE INDEX IF NOT EXISTS idx_dsl_date ON daily_send_log(mailbox_id, send_date);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version         INTEGER NOT NULL,
    applied_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
);

INSERT OR IGNORE INTO schema_version (version) VALUES (1);
