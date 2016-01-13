from expiringdict import ExpiringDict
import time
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
        
class ExpiringDictWithCallback(ExpiringDict):
    def __init__(self, max_len, max_age_seconds, callback=None):
        ExpiringDict.__init__(self, max_len, max_age_seconds)
        self.callback = callback

    def __contains__(self, key):
        # Return True if the dict has a key, else return False.
        try:
            with self.lock:
                item = OrderedDict.__getitem__(self, key)
                if time.time() - item[1] < self.max_age:
                    return True
                else:
                    if self.callback:self.callback(item[0])
                    del self[key]
        except KeyError:
            pass
        return False
    
    def __getitem__(self, key, with_age=False):
        '''
        Return the item of the dictionary
        Raises a KeyError if key is not in the map.
        '''
        with self.lock:
            item = OrderedDict.__getitem__(self, key)
            item_age = time.time() - item[1]
            if item_age < self.max_age:
                if with_age:
                    return item[0], item_age
                else:
                    return item[0]
            else:
                if self.callback: self.callback(item[0])
                del self[key]
                raise KeyError(key)
