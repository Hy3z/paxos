import numpy as np
import matplotlib.pyplot as plt

# Election timeout values (in milliseconds)
tle_values = np.array([0.5, 1.0, 1.5, 2.0]) * 1000

# Latence moyenne (ms) pour différentes valeurs de α
latency_alpha_0 = np.array([504, 625, 612, 636])
latency_alpha_01 = np.array([532, 956, 1281, 1277])
latency_alpha_1 = np.array([518, 1052, 1074, 1210])

# Deuxième graphe adapté pour représenter les mêmes données
alpha_values = np.array([0, 0.1, 1])
latency_tle_05 = np.array([latency_alpha_0[0], latency_alpha_01[0], latency_alpha_1[0]])
latency_tle_1  = np.array([latency_alpha_0[1], latency_alpha_01[1], latency_alpha_1[1]])
latency_tle_15 = np.array([latency_alpha_0[2], latency_alpha_01[2], latency_alpha_1[2]])
latency_tle_2  = np.array([latency_alpha_0[3], latency_alpha_01[3], latency_alpha_1[3]])

# Graphe
plt.figure(figsize=(8, 5))
plt.plot(alpha_values, latency_tle_05, label='tle = 0.5s', marker='o')
plt.plot(alpha_values, latency_tle_1, label='tle = 1.0s', marker='s')
plt.plot(alpha_values, latency_tle_15, label='tle = 1.5s', marker='^')
plt.plot(alpha_values, latency_tle_2, label='tle = 2.0s', marker='x')

plt.title("Impact of Crash Probability on Latency (N = 60)")
plt.xlabel("Crash Probability (α)")
plt.ylabel("Average Latency (ms)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
