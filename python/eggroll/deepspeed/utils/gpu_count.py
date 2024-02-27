import pynvml

def gpu_count():
    try:
        pynvml.nvmlInit()
        return pynvml.nvmlDeviceGetCount()
    except Exception as e:
        return 0

if __name__ == '__main__':
    print(gpu_count())