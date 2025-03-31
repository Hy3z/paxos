import numpy as np
import matplotlib.pyplot as plt

# Nombre de processus N
N_values = np.array([3, 10, 100])

# Latence moyenne (en ms) pour différentes valeurs de α (proba de crash)
latency_alpha_0   = np.array([2, 12, 1035])
latency_alpha_01  = np.array([1, 29, 1516])
latency_alpha_1   = np.array([2, 20, 1523])

# Création du graphe
plt.figure(figsize=(9, 5))
plt.plot(N_values, latency_alpha_0, label='α = 0', marker='o')
plt.plot(N_values, latency_alpha_01, label='α = 0.1', marker='s')
plt.plot(N_values, latency_alpha_1, label='α = 1', marker='^')

# Ligne horizontale rouge à 1500ms (seuil de timeout/hold)
plt.axhline(y=1500, color='red', linestyle='--', label='t_le (1500ms)')

plt.title("Impact of Number of Processes on Latency (tle = 1.5s)")
plt.xlabel("Number of Processes (N)")
plt.ylabel("Average Latency (ms)")
plt.ylim(0, 1800)  # dézoomer un peu pour mieux voir la ligne rouge
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
