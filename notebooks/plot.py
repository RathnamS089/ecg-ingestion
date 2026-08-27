import wfdb
import matplotlib.pyplot as plt
record=wfdb.rdrecord('../mitbh/100')
duration_sec=10
samples=duration_sec*record.fs
ecg=record.p_signal[:samples,:]
time=[i/record.fs for i in range(samples)]
plt.figure(figsize=(12,6))
for i,lead_name in enumerate(record.sig_name):
    plt.subplot(len(record.sig_name),1,i+1)
    plt.plot(time,ecg[:,i],linewidth=0.6)
    plt .title(f"Lead:{lead_name}")
    plt.ylabel("mV")
    plt.grid(True,alpha=0.1)
plt.xlabel("Time (seconds)")
plt.tight_layout()
plt.show()
