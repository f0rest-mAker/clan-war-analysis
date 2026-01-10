import asyncio
import datetime
from typing import Any, AsyncIterator, Tuple, Sequence

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.sensors.base import BaseSensorOperator
from airflow.utils import timezone


class CustomDateTimeTrigger(BaseTrigger):
    def __init__(self, target_time: datetime.datetime, check_interval: int = 60):
        super().__init__()
        self.target_time = target_time
        self.check_interval = check_interval
    
    def serialize(self) -> Tuple[str, dict[str, Any]]:
        # Отвечает за сохранение состояния триггера в БД
        return (
            "custom_sensors.date_time_sensor.CustomDateTimeTrigger", # Путь импорта для триггера
            {
                "target_time": self.target_time,
                "check_interval": self.check_interval # Передаем то, что нужно для триггера
            }
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while self.target_time > timezone.utcnow():
            await asyncio.sleep(self.check_interval)
        yield TriggerEvent(True)


class CustomDateTimeSensorAsync(BaseSensorOperator):
    template_fields: Sequence[str] = ("target_time",)

    def __init__(self, target_time: str, check_interval: int = 60, **kwargs):
        super().__init__(**kwargs)
        self.check_interval = check_interval
        self.target_time = target_time
    
    def execute(self, context) -> None:
        if (target_time := datetime.datetime.fromisoformat(self.target_time)) > timezone.utcnow():
            self.defer(
                trigger=CustomDateTimeTrigger(
                    target_time=target_time,
                    check_interval=self.check_interval
                ),
                method_name="execute_complete",
            )
        else:
            self.log.info("Целевое время уже наступило.")
    
    def execute_complete(self, context, event: Any = None) -> None:
        self.log.info("Событие наступило, продолжаем выполнение DAG.")