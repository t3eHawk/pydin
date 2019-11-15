from .log import make_log
from .queue import make_queue

class Step():
    def __init__(self, input, output=None, bind=None):
        self.log = make_log()

        self.input = input
        self.output = output
        self.bind = bind

        self.is_trunk = not input.is_executable
        self.is_action = input.is_executable

        self.queue = make_queue()
        pass

    def __str__(self):
        if self.output is not None:
            return f'{self.input} --> {self.output}'
        else:
            return f'--> {self.input} -->'

    __repr__ = __str__

    def run(self):
        self.log.info(f'Step {self} started')
        if self.is_trunk is True and self.is_action is False:
            self.input.assign(step=self)
            self.output.assign(step=self)

            self.input.extract()
            self.output.load()

            self.input.stop()
            self.queue.join()
            self.output.stop()

        elif self.is_trunk is False and self.is_action is True:
            self.input.assign(step=self)
            self.input.execute()
        self.log.info(f'Step {self} finished')
        pass
