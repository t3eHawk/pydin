"""Contains application variables."""

import sqlalchemy as sa


class KeyField:
    """Represent base class for key fields."""

    def __call__(self, model):
        self.model = model
        return self

    def __repr__(self):
        return self.name

    @property
    def name(self):
        value = ''
        for i, elem in enumerate(self.__class__.__name__):
            if elem.isupper() and i > 0:
                value += '_'
            value += elem.lower()
        return value

    @property
    def label(self):
        return f'pd_{self.name}'

    @property
    def column(self):
        return sa.literal(self.value).label(self.label)

    def associated(self, obj):
        """Check if the key field associated with the given instance."""
        class_name = obj.__class__.__name__
        if self.name.startswith(class_name.lower()):
            return True
        elif self.name.startswith('run') and class_name == 'Job':
            return True
        elif self.name.startswith('process') and class_name == 'Job':
            return True
        else:
            return False


class RunId(KeyField):
    """Represents Run ID as a key field."""

    @property
    def value(self):
        if self.model.job:
            return self.model.job.record_id


class ProcessId(RunId):
    """Represents Process ID as a key field."""

    pass


class TaskId(KeyField):
    """Represents Task ID as a key field."""

    @property
    def value(self):
        if self.model.pipeline:
            return self.model.pipeline.task.id


class StepId(KeyField):
    """Represents Step ID as a key field."""

    @property
    def value(self):
        if self:
            raise NotImplementedError


run_id = ProcessId()
task_id = TaskId()
step_id = StepId()
process_id = ProcessId()
