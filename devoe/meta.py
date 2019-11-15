class Fetchable():

    def fetchone(self):
        chunk = self.answerset.fetchone()
        if chunk:
            self.selected += 1
            self.log.info(f'{self.selected} records fetched')
        return chunk

    def fetchmany(self):
        chunk = self.answerset.fetchmany(self.fetch)
        if chunk:
            self.selected += len(chunk)
            self.log.info(f'{self.selected} records fetched')
        return chunk

    def fetchall(self):
        chunk = self.answerset.fetchall()
        if chunk:
            self.selected += len(chunk)
            self.log.info(f'{self.selected} records fetched')
        return chunk

class Extractable():

    is_extractable = True

    def extract():
        raise NotImplementedError('method <load> must be overrided')
        pass

    def extractor(self):
        raise NotImplementedError('method <load> must be overrided')
        pass

class Loadable():

    is_loadable = True

    def load():
        raise NotImplementedError('method <load> must be overrided')
        pass

    def loader(self):
        raise NotImplementedError('method <load> must be overrided')
        pass

class Executable():

    is_executable = True

    def execute(self):
        raise NotImplementedError('method <execute> must be overrided')
        pass

class Unextractable():

    is_extractable = False

class Unloadable():

   is_loadable = False

class Unexecutable():

    is_executable = False   
