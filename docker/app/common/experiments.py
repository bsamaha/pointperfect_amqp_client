from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

class Experiment:
    def __init__(self) -> None:
        self.alias = None
        self.configuration = None
        self.description = None
        self.end_time = None
        self.experiment_id = None
        self.experiment_type = None
        self.region = None
        self.start_time = None

    def set_default_attributes(self) -> None:
        self.alias = None
        self.configuration = None
        self.description = None
        self.end_time = None
        self.experiment_id = None
        self.experiment_type = 'unknown'
        self.region = None
        self.start_time = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return vars(self)

    def update_from_config(self, config):
        logger.info('Updating Experiment object from config: %s', config)
        logger.info('Current Experiment object: %s', self.to_dict())
        for attr in vars(self):
            if hasattr(config, attr):
                setattr(self, attr, getattr(config, attr))

