from optparse import OptionParser
import types

def keys(self):
    return self.__dict__.keys()

def __getitem__(self, item):
    return self.__dict__[item] 

def __setattr__(self,name,value):
    self.__dict__[name] = value

class DictOptionParser(OptionParser):
    def parse_args(self,argv):
        (options, args) = OptionParser.parse_args(self, argv)
        #bind keys, __getitem__ and __setattr__ methods to options so it can serve as a dict
        options.keys = types.MethodType(keys,options)
        options.__getitem__ = types.MethodType(__getitem__,options)
        options.__setattr__ = types.MethodType(__setattr__,options)
        return (options, args)
    

    
