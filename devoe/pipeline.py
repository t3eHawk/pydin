from .task import Task
from .step import Step
from .items import Item

class Pipeline(Task):
    def __init__(self, root, *args, **kwargs):
        super().__init__(**kwargs)
        if isinstance(root, Item) is True:
            self.root = root
            self.root.assign(bind=self)
            self.all_items = {self.root.title: self.root}
            self.all_steps = []
            self.append(*args)
        else:
            msg = [
                f'root must be a {Item.__name__}',
                f'not {root.__class__.__name__}']
            msg = ', '.join(msg)
            raise TypeError(msg)
        pass

    def append(self, *args, source=None):
        source = self.root if source is None else self.all_items[source]
        for i, item in enumerate(args):
            if isinstance(item, Item) is False:
                msg = [
                    f'root must be a {Item.__name__}',
                    f'not {root.__class__.__name__}']
                msg = ', '.join(msg)
                raise TypeError(msg)
            else:
                item = item.assign(bind=self)

                item.source = source if i == 0 else args[i - 1]
                item.source.targets.append(item)

                title = item.title
                number = 1
                while True:
                    if self.all_items.get(title) is not None:
                        if self.all_items.get(title) == item:
                            break
                        else:
                            number += 1
                            new_title = f'{title} {number}'
                            if self.all_items.get(new_title) is None:
                                item.title = new_title
                                self.all_items[new_title] = item.title
                                break
                    else:
                        self.all_items[title] = item.title
                        break
                self.all_items[item.title] = item
        pass

    def walk(self):
        for item in self.all_items.values():
            self.log.debug(f'Creating steps with {item}')
            if item.is_extractable is True:
                for target in item.targets:
                    if target.is_loadable is True:
                        step = Step(item, output=target, bind=self)
                        self.all_steps.append(step)
            elif item.is_executable is True:
                step = Step(item, bind=self)
                self.all_steps.append(step)
        return self.all_steps

    def execute(self):
        for step in self.walk():
            step.run()
        pass
