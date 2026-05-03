from dagster import ScheduleDefinition

from .jobs import daily_analytics_job

daily_schedule = ScheduleDefinition(
    job=daily_analytics_job,
    cron_schedule="0 2 * * *",
    description="Run daily analytics at 2 AM UTC",
)
