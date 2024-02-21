import logging

logger = logging.getLogger(__name__)

class Hardware:
    def __init__(self) -> None:
        self.alias = None
        self.region = None
        self.location_desc = None
        self.owner_name = None
        self.make = None
        self.model = None
        self.signals = None
        self.configuration = None
        self.hardware_id = None
        self.attributes = {"experiment_id": 1}

    def set_default_attributes(self):
        self.alias = None
        self.region = None
        self.location_desc = None
        self.owner_name = None
        self.make = None
        self.model = None
        self.signals = None
        self.configuration = None
        self.hardware_id = None
        self.attributes = {"experiment_id": 1}

    def to_dict(self):
        return vars(self)

    def update_from_config(self, config):
        for attr in vars(self):
            if hasattr(config, attr):
                setattr(self, attr, getattr(config, attr))
