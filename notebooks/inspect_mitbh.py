import wfdb
record=wfdb.rdrecord('../mitbh/100')
print("Lead (signal names):",record.sig_name)
print("Sample rate(Hz):",record.fs)
print("Total samples per lead:",record.sig_len)
print("Duration (seconds):",record.sig_len/record.fs)
print("Duration (minutes):",record.sig_len/record.fs/60)
print("Units:",record.units)