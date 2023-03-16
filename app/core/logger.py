####################################################################
class ErrorWrapper:


    ERRORS = {}

    MESSAGES = {}

    
    ################################################################
    def __init__(self, on_error = None, on_notice = None):
        self.on_error = on_error
        self.on_notice = on_notice


    ################################################################
    def err(self, code, *details):
        if self.on_error:
            msg = []
            if code and code in self.__class__.ERRORS:
                msg.append(self.__class__.ERRORS[code])
            if details:
                msg.extend(details)
            if msg:
                self.on_error(' : '.join(msg))


    ################################################################
    def msg(self, code, *details):
        if self.on_notice:
            msg = []
            if code and code in self.__class__.MESSAGES:
                msg.append(self.__class__.MESSAGES[code])
            if details:
                msg.extend(details)
            if msg:
                self.on_notice(' : '.join(msg))
