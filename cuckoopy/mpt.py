import matplotlib.pyplot as plt
import numpy as np
from cuckoopy import query

def plot(*args, **kwargs):
  data = query(*args, **kwargs)
  if isinstance(data, list):
    x = np.array([entry[0] for entry in data])
    y = np.array([entry[1] for entry in data])
  else:
    raise Exception('Unsupported for nested data')

  plt.plot(x,y)
  plt.show()
