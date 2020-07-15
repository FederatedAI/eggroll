import re
import subprocess

def sub_dict(form_dict, sub_keys, default=None):
	return dict([(k.strip(), form_dict.get(k.strip(), default)) for k in sub_keys.split(',')])


def query_file(file_name, opts=''):
	mem_info = {}
	print(file_name)
	with open(file_name, 'r') as f:
		data = f.readlines()
		for i in data:
			if ':' in i or '=' in i:
				i = i.replace(':', ',').replace('=', ',')
				k, v = [x.strip() for x in i.split(',')]
				mem_info[k] = int(v.split()[0])
	return sub_dict(mem_info, opts)


def query_cmd(cmd, opts=''):
	if opts:
		opts = " | grep -E '" + opts.replace(',', '|').replace(' ', '') + "'"
	print(cmd + opts)
	p = subprocess.Popen(cmd + opts, stdout=subprocess.PIPE, shell=True)
	return p.communicate()[0]
	
def query(cmd, opts='', flags=True):
	if flags:
		print(str(query_cmd(cmd, opts)))
	else:
		print(str(query_file(cmd, opts)))

if __name__ == "__main__": 
	max_user_processes_params=[('cat /proc/sys/kernel/threads-max',),('/etc/sysctl.conf', 'kernel.pid_max', False),('cat /proc/sys/kernel/pid_max',),('cat /proc/sys/vm/max_map_count',)]
	print('==============max user processes===============')
	for p in max_user_processes_params:
		s = query(*p)
	
	max_files_count_params=[('cat /etc/security/limits.conf', 'nofile'),('cat /etc/security/limits.d/80-nofile.conf',),('/etc/sysctl.conf','fs.file-max', False),('cat /proc/sys/fs/file-max',)]
	print('===============max files count=================')
	for i in max_files_count_params:
		query(*i)
	
	memory_params=('/proc/meminfo', 'MemTotal, MemFree, MemAvailable, SwapTotal, SwapFree', False)
	print('================memory info====================')
	query(*memory_params)

	disk_params=('df -lh', '/dev/vdb,/dev/vda1')
	print('================disk info====================')
	query(*disk_params)
 
