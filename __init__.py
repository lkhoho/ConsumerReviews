import sys
import platform

if platform.system() == 'Windows':
    sys.path.append('D:\\Developer\\ConsumerReviews')
elif platform.system() == 'Linux':
    sys.path.append('/home/keliu/Developer/python/ConsumerReviews')
elif platform.system() == 'Darwin':
    sys.path.append('/Users/keliu/Developer/python/ConsumerReviews')
else:
    raise ValueError('Unsupported platform: ' + platform.system())
