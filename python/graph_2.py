# Reimporting necessary libraries after code execution environment reset
import numpy as np
import matplotlib.pyplot as plt

# Election timeout values (in milliseconds)
tle_values = np.array([0.5, 1.0, 1.5, 2.0]) * 1000

# Latence moyenne (ms) pour différentes valeurs de α
latency_alpha_0 = np.array([504, 625, 612, 636])
latency_alpha_01 = np.array([532, 956, 1281, 1277])
latency_alpha_1 = np.array([518, 1052, 1074, 1210])

# Graphe
plt.figure(figsize=(8, 5))
plt.plot(tle_values, latency_alpha_0, label='α = 0', marker='o')
plt.plot(tle_values, latency_alpha_01, label='α = 0.1', marker='s')
plt.plot(tle_values, latency_alpha_1, label='α = 1', marker='^')

# Ajout de la ligne x = y
plt.plot(tle_values, tle_values, color='red', linestyle='--', label='x = tle')

plt.title("Impact of Leader Election Timeout on Latency (N = 60)")
plt.xlabel("Leader Election Timeout (ms)")
plt.ylabel("Average Latency (ms)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
