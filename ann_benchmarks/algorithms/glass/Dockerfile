FROM ann-benchmarks

RUN lscpu
RUN apt-get update && apt-get install -y build-essential git python3 python3-distutils python3-venv
RUN git clone https://github.com/zilliztech/pyglass.git
RUN pip3 install pybind11
RUN cd pyglass && cd python && rm -rf build && python3 setup.py bdist_wheel && pip3 uninstall glassppy -y && cd dist && ls | xargs pip install
RUN python3 -c 'import glassppy as glass'