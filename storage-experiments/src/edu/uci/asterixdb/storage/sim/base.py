import numpy as np
import matplotlib.pyplot as plt
import pyqt_fit as fit

xs = [0.001, 0.01, 0.1, 0.2, 0.3, 0.4]
ys = [5, 3, 2, 2, 2, 2]

k0 = fit.nonparam_regression.NonParamRegression(xs, ys, method=fit.npr_methods.SpatialAverage())
k0.fit()
plt.plot(xs, ys, label="Actual", linewidth=2)
plt.plot(xs, k0(xs), label="Spatial Averaging", linewidth=2)
plt.legend(loc='best')
plt.show()

