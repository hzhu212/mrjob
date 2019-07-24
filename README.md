> 注：这是一个为了深入理解和学习 Map-Reduce 计算以及 Hadoop 的原理而创建的练手项目，严重参考了项目 [mrjob](https://pythonhosted.org/mrjob/index.html)，相当于实现了该项目的 minimal 版本。
> 
> 如果你需要在工程中使用 Map-Reduce，那么请跳转到原项目 [mrjob](https://pythonhosted.org/mrjob/index.html)。
> 
> 如果你想要了解 Map-Reduce 框架的内部设计，那么这个项目能提供一定的参考。

# 基于Python的Map-Reduce计算框架

## 1. 简介

由于工作中经常需要使用 Python 为 hadoop streaming 作业编写 mapper/reducer 函数，考虑到代码的可靠性、可读性、高效性等，特意编写了此 Map-Reduce 计算框架。

该框架的目的是帮助用户简单、高效地实现 Map-Reduce 作业。

该框架的名称为 mrjob，因为其 API 设计风格借鉴了开源库 [mrjob](https://pythonhosted.org/mrjob/index.html)。

借助 mrjob 实现的一个简单的 WordCount 示例 `wc.py`：

```python
from mrjob import MRJob

class WordCount(MRJob):

    def mapper(self, _, line):
        yield 'line', 1
        yield 'word', len(line.strip().split())
        yield 'char', len(line)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    job = WordCount()
    job.run(runner='local', input='data.txt')
```

执行 `python wc.py` 命令将在本地开始 Map-Reduce 作业，并输出类似下面的结果：

```
char	319
line	14
word	32
```

你还可以像这样调用该脚本：

```sh
# -input 指定输入文件(可多个、可包含通配符)
python wc.py -input data.txt

# 将 input 设定为 "-" 代表从 stdin 中读取数据
cat data.txt | python wc.py -input -

# 不指定 input，则默认为 stdin
python wc.py < data.txt
```

在上述脚本中，你大概注意到了 `runner='local'` 这一启动参数，这个参数意味着我们只是在本地模拟 Map-Reduce 的计算过程。在真实环境中，Map-Reduce 作业往往要交给 Hadoop 集群等更强大的计算资源来处理。我们会在【使用方法】一节中详细介绍如何将该作业提交到 Hadoop 集群。

## 2. 设计思想

mrjob 的核心概念包含两个部分：

- MRJob: 负责定义 Map-Reduce 计算逻辑；
- Runner: 负责调用计算资源。

在实际使用中，Runner 只负责幕后工作，对用户是完全不可见的。因此用户不必了解 Runner 的实现方式，而只需要继承 `MRJob` 类，并实现相应的 `mapper/combiner/reducer` 方法即可。

目前已经实现的 Runner 类包括：

1. `LocalRunner`: 在本地运行，使用本地计算资源。
2. `HadoopRunner`: 在 hadoop 集群上运行。是当前的默认 Runner。

其中 `LocalRunner` 是为了调试 mapper/reducer 逻辑而专门设计的。当作业编写完成后，强烈推荐先使用 `LocalRunner` 运行一下。如果你不经调试而直接把作业提交到 hadoop 集群，你很可能会为每个幼稚的错误花费十分钟的代价。

由于 `LocalRunner` 使用的是本地计算资源，因此对输入数据的大小进行了限制（<50MB 且 <500000行），如果数据量超限，程序或者抛出错误、或者忽略后续的数据。所以，你可以放心调试，不必担心因输入数据过多而把电脑卡死。

## 3. 使用方法

在使用之前，你需要先安装 mrjob 包。请 clone 或下载此项目，进入项目目录，执行 `pip install .` 即可（或者安装为 editable 模式：`pip install -e .`）。

mrjob 目前是基于 Python2.7 实现的。

### 3.1 编写子类继承 `MRJob`

具体继承方式可以参考本文开头的例子，可重写的方法包括 `mapper`、`combiner`、`reducer`。它们的输入参数和返回值均为 `key, value` 形式，其中 `key` 为 Map-Reduce 内部运算过程中的排序字段。

在重写 mapper/combiner/reducer 方法时，有以下三点需要注意：

1. 由于要从输入文件中读取数据，`mapper` 的 `key` 参数总为 `None`，而 `value` 则为 `line`（代表从输入流中读取的一行，已去除行尾换行符）。由于参数 `None` 对于我们毫无意义，只是为了保持函数参数形式的统一，所以示例中的 `mapper` 方法的接收参数被写作 `_, line`，表示我们不 care 第一个参数。

2. 由于要向输出文件中写入数据，`reducer` 与 `mapper` 的过程刚好相反，你可以把 `key` 或 `value` 置为 `None` 来避免把它们写入输出文件。

    为了提高 reducer 的灵活性，我们做了如下规定：如果 yield 出的 `key`、`value` 均非 `None`，那么将把 `key` 和 `value` 组成一个数组，然后执行 flatten 操作，最后通过 `\t` 拼接并写入输出文件。整个过程相当于 `'\t'.join(flatten([key, value]))`。这样会带来很高的易用性，例如：`yield 'foo', [1, 2, 3]`、`yield ['foo'], [1,2,3]`、`yield ['foo'], [1,[2,3]]` 将会得到同样的输出，即 `foo\t1\t2\t3`，这是符合用户预期的，因此用户很少需要显式地使用 `\t` 连接字符串。

3. 由于 `combiner`、`reducer` 的输入都是前一步的输出经过 **排序** 和 **聚合** 后的结果，所以它们接收的 `key` 为单个值，而 `value` 却是一个序列（`generator`），代表上一步的输出中拥有相同 `key` 的所有 `value` 的列表。这也是为什么示例中会把 `reducer` 的参数写作 `key, values`（`value` 后加了 `s`，表示复数）。

在整个运算过程中，字符串一直保持为 `bytes` 类型，因此不涉及编解码问题（输入的是什么编码，输出的还是同一种编码）。然而，如果你在程序中创造了新的字符串，并希望写入文件，这就涉及到了编码统一性的问题。mrjob 内部默认使用 UTF-8 编码，所以为了减少编码出错的可能性，请务必在所有文件中统一使用 **UTF-8** 编码。

### 3.2 启动 MRJob

当我们编写了一个 `MRJob` 的子类，其实只是规定了 Map-Reduce 的计算逻辑，而并未说明如何调用计算资源。这一任务由背后的 Runner 类自动完成。Runner 类对用户是不可见的，用户也不需要关心其实现。用户需要做的仅仅是调用 `MRJob` 子类的 `run` 方法，并指定 `runner` 参数，例如：

```python
SomeJob().run(runner='{runner_name}')
```

在之前的例子中，我们已经演示了如何使用 `LocalRunner`，现在我们演示如何把例子中的 Map-Reduce 任务提交给 hadoop 集群。只需要修改一下 `run` 方法的参数即可：

```python
if __name__ == '__main__':
    WordCount().run(
        # 'hadoop' is the default runner
        input='hdfs://localhost:9902/user/zhuhe212/common/feed_os_version.txt',
        output='hdfs://localhost:9902/user/zhuhe212/tmp/test_mrjob/',
        jobconf={
            'mapred.job.name': 'zhuhe212_word_count_by_mrjob',
            'mapred.reduce.tasks': 1,
            'mapred.job.queue.name': 'queuename1',
        })
```

通过这段简短的代码，我们相当于构造了如下的 hadoop streaming 命令（运行脚本时会打印在控制台中），且构造过程由 `HadoopRunner` 在幕后自动帮我们完成了：

```sh
/home/zhuhe212/hadoop/bin/hadoop streaming  \
    -D mapred.job.tracker=jobtracker1.domain:54311  \
    -D mapred.job.map.capacity=4000  \
    -D mapred.job.queue.name=queuename1  \
    -D mapred.job.name=zhuhe212_word_count_by_mrjob  \
    -D mapred.reduce.tasks=1  \
    -D mapred.job.reduce.capacity=800  \
    -D mapred.job.priority=NORMAL  \
    -input hdfs://localhost:9902/user/zhuhe212/common/feed_os_version.txt  \
    -output hdfs://localhost:9902/user/zhuhe212/tmp/test_mrjob__tmp_mrjob  \
    -cacheArchive 'hdfs://localhost:9902/user/zhuhe212/common/python2.7.tar.gz#python2.7.1'  \
    -file /home/zhuhe212/workspace/mrjob/mrjob/bundle/mrjob.py  \
    -file test/wc.py  \
    -mapper 'python2.7.1/python/bin/python "wc.py" --mapper'  \
    -reducer 'python2.7.1/python/bin/python "wc.py" --reducer'
```

PS: 你不必考虑何时清理 output 目录的问题，`HadoopRunner` 会在背后自动处理。并且如果本次任务不幸失败了，那么旧的 output 目录会原样保留，不会被删除，就像 hive 中 `INSERT OVERWRITE DIRECTORY` 语句所做的那样。

生成的 shell 命令中，除了我们设置的参数外，也有一些由 `HadoopRunner` 自动补全的默认参数，比如 mapper、reducer、mapred.job.priority 等。在 `MRJob.run` 方法中设置的同名参数将会覆盖默认参数。

另外一个值得注意的细节是，`-cacheArchive` 选项中的 Python 包，以及 `-mapper`、`-reducer` 选项中的 Python 可执行文件路径都是由 mrjob 自动填充的。这些默认路径很可能不符合你的需要，你可以在调用 `run` 方法之前通过如下方式设置集群上的 Python 环境：

```python
if __name__ == '__main__':
    from mrjob.runner.hadoop import set_hadoop_python
    set_hadoop_python(
        'hdfs://localhost:9902/user/zhuhe212/common/python272.tar.gz#python2.7.2',
        'python2.7.2/python2.7/bin/python')
    # 通过 help(set_hadoop_python) 可查看该函数的文档

    WordCount().run(...)
```

`MRJob.run` 方法支持的参数几乎与 [hadoop streaming 官方文档中的参数列表](https://hadoop.apache.org/docs/current/hadoop-streaming/HadoopStreaming.html#Streaming_Command_Options) 如出一辙，因此不增加任何学习成本，只有一些极不常用的参数未做支持，详情参见【参考手册】一节。

#### 3.2.1 使用命令行参数

把所有参数写死在脚本中显然不是一个好设计。比如在上例中，假如用户想临时更换一个队列（`mapred.job.queue.name`）执行任务，难道还要编辑脚本、并在执行任务之后再复原？这样显然不太方便。

为了解决这个问题，mrjob 也支持调用时在命令行中设置参数，并且命令行参数会覆盖 `MRJob.run` 方法中设置的参数。对于上面的例子，我们可以通过如下命令临时更改一些设置：

```sh
python wc.py -input /user/zhuhe212/tmp/another_test -D mapred.job.queue.name=tianqi-ubs-pv -D mapred.job.priority=HIGH
```

同样地，命令行参数的格式也与 hadoop streaming 命令保持一致，不增加学习成本。

### 3.3 高级用法

在讲解高级用法之前，列举几点**注意事项**：

- 如果你的代码中除了 mapper、reducer 之外，还用到了 combiner 方法，那么在提交到 hadoop 集群时，务必设置 jobconf 项 `-D dce.shuffle.enable=false`，否则 combiner 方法将不会生效。该选项中的 DCE 指的是百度自研和优化的分布式计算引擎，至于为何会屏蔽 combiner 过程，就不得而知了。

#### 3.3.1 使用 init 和 final 方法

在某些任务中，我们常常需要在 mapper/combiner/reducer 执行前后进行一些初始化工作和收尾工作，比如载入文件、做一些额外统计等。这种情况下推荐使用 init 和 final 方法。

mrjob 支持如下的 init 和 final 方法：

- `mapper_init()`
- `mapper_final()`
- `combiner_init()`
- `combiner_final()`
- `reducer_init()`
- `reducer_final()`

其中，每个 `xxx_init`、`xxx_final` 分别在 `xxx` 方法运行之前和之后执行**仅一次**。`xxx_init`、`xxx_final` 方法不接受任何参数，它们的返回值也是可选的。如果它们有 `yield` 值，那么其格式需要与 `xxx` 方法的 `yield` 值保持一致，mrjob 会把它们的 `yield` 值做完全相同的处理。

另外，在 init 和 final 方法中声明的属性，其作用域是**自限**的，即只能在当前 mapper/combiner/reducer 域中使用。例如，在 `mapper_init` 中声明属性 `self.a = 0`，则只能在 `mapper_init`、`mapper` 和 `mapper_final` 中使用该属性。在其他方法如 `combiner_init`、`reducer`、`reducer_final` 等中引用该属性将报错。

举个例子，考虑如下需求：

我们需要统计一段文本 `data.txt` 中的**部分单词**出现的次数，目标词汇表定义在文件 `word_list.txt` 中。同时，要求把所有的 be 动词统计到一个条目 `verb_be` 之下，而不是每个 be 动词分别统计。

针对这个需求，我们可以借助 init 和 final 方法设计出如下代码：

```python
import re

from mrjob import MRJob

BE_SET = {'am', 'is', 'are', 'was', 'were', 'be', 'being', 'been'}

class WordCountPro(MRJob):
    """统计指定的词的出现次数，并将所有的 be 动词汇总到一个词条下"""

    def mapper_init(self):
        # 载入限定的词汇表
        self.word_set = set()
        with open('word_list.txt', 'r') as f:
            for line in f:
                self.word_set.add(line.strip())
        self.word_set.update(BE_SET)

    def mapper(self, _, line):
        for word in re.findall(r'\w+', line.strip()):
            # 只统计词汇表中的单词
            if word in self.word_set:
                yield word, 1

    def reducer_init(self):
        # 初始化 be 动词计数
        self.be_count = 0

    def reducer(self, key, values):
        if key not in BE_SET:
            yield key, sum(values)
        else:
            # be 动词单独统计
            self.be_count += sum(values)

    def reducer_final(self):
        # 将 be 动词作为一个条目输出
        yield 'verb_be', self.be_count

if __name__ == '__main__':
    job = WordCountPro()
    job.run(runner='local', input='data.txt')
```

假设 `data.txt` 包含的文本如下：

```
I am in Beijing now, but I was in Shanghai the day before yesterday
It is a good day today
```

`word_list.txt` 中包含的词汇列表如下：

```
day
now
```

那么程序将会输出：

```
day	2
now	1
verb_be	3
```

**是否非要使用 init 和 final 不可？**

事实上，在上面这个例子中，你也可以把 init 方法中初始化的变量 `word_set`、`be_count` 在全局作用域中初始化（就像变量 `BE_SET` 那样），这样程序同样能够正常工作。但是**不建议**这样做，有如下两个原因：

1. 全局作用域中的语句会被重复执行多次（job初始化、mapper、combiner、reducer 过程中分别执行一次），降低计算效率。在上面的例子中，假如 `word_list.txt` 文件非常大，那么我们会浪费很多时间和资源。而使用 init 方法可以保证任何初始化语句只执行一次。
2. 全局变量会降低代码的可读性。只有真正需要全局使用的变量，才应该声明为全局变量。比如例子中的 `BE_SET`，既在 `mapper` 中使用，又在 `reducer_final` 中使用。而 `word_set` 变量仅用在 `mapper` 中，因此在 `mapper_init` 中声明是更好的选择。

#### 3.3.2 使用 `is_launched` 方法

我们启动作业的时候常常需要动态设定一些参数，最常见的比如日期。以前文的 WordCount 为例，我们可以通过如下方式添加日期参数：

```python
from mrjob import MRJob

class WordCount(MRJob):
    # ...
        
def parse_datetime_from_cli():
    """从命令行中解析日期参数"""
    import argparse
    from dateutil import parser as dateparser
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', dest='date', required=True)  # 要求用户必须传入 date 参数
    args, unrecognized = parser.parse_known_args()
    sys.argv[1:] = unrecognized  # date 参数不应该影响后续参数的解析
    dt = dateparser.parse(args.date)
    return dt

if __name__ == '__main__':
    job = MyJob()

    dt = parse_datetime_from_cli()
    date_str = dt.strftime('%Y%m%d')

    job.run(
        runner='hadoop', 
        input='/user/zhuhe212/tmp/some_data/{}'.format(date_str))
```

这段代码启动起来完全没有问题，但将会在集群上执行 map-reduce 时失败。失败原因非常明显，还记得吗？我们是通过如下命令来调用 hadoop streaming 的：

```sh
/home/zhuhe212/hadoop/bin/hadoop streaming \
    -mapper 'python2.7.1/python/bin/python "wc.py" --mapper' \
    -reducer 'python2.7.1/python/bin/python "wc.py" --reducer' \
    # ...
```

当集群执行 `python2.7.1/python/bin/python "wc.py" --mapper` 命令时，会因为缺少必选参数 `date` 而失败。然而集群并不需要 `date` 参数，因为我们的作业在初始化过程中已经使用了 `date` 参数。因此我们需要一种方法来让集群绕过初始化代码，`MRJob.is_launched` 方法就是为这种场景设计的。你可以这样重写以上代码：

```python
if __name__ == '__main__':
    job = MyJob()

    # 如果作业已经提交，那么直接运行，然后退出脚本
    if job.is_launched():
        job.run()
        exit()

    # 否则，提交作业
    dt = parse_datetime_from_cli()
    date_str = dt.strftime('%Y%m%d')

    job.run(
        runner='hadoop', 
        input='/user/zhuhe212/tmp/some_data/{}'.format(date_str))
```

PS: `is_launched` 是一个静态方法，调用时可以绑定、也可以不绑定具体的对象，以下几种调用方式是等效的： 

- `MRJob.is_launched()`
- `MRJob().is_launched()`
- `SomeJob.is_launched()`
- `SomeJob().is_launched()`

#### 3.3.3 本地快速调试

**推荐对每一个 hadoop streaming 作业执行一次本地调试**。如果你通过“提交作业->报错->查看作业日志”的方式来调试，将会非常耗时。

继续上一小节 `wc.py` 的例子，目前我们已经完成了整个脚本，只需要插入简短的两行代码即可开启本地调试模式：

```python
if __name__ == '__main__':
    job = MyJob()

    # 如果作业已经提交，那么直接运行，然后退出脚本
    if job.is_launched():
        job.run()
        exit()

    # 在这里插入两行代码
    # --------------------------------------------------------------------------
    job.run(runner='local')
    exit()
    # --------------------------------------------------------------------------

    # 否则，提交作业
    dt = parse_datetime_from_cli()
    date_str = dt.strftime('%Y%m%d')

    job.run(
        runner='hadoop', 
        input='/user/zhuhe212/tmp/some_data/{}'.format(date_str))
```

然后像这样调用脚本：

```sh
hadoop fs -cat /user/zhuhe212/tmp/some_data/*/* | head | python wc.py
# 如果你把 HDFS 上的数据事先保存到本地，将会更快！
```

这样就会立即使用少量的数据完成对 mapper/reducer 等函数的调试，你将直接在控制台中看到 reducer 的输出结果。调试完成后，只需要注释掉这两行代码，就可以正常提交作业了。

在调试过程中，如果你需要查看一些中间变量，推荐使用 `logging` 模块。示例如下：

```python
import logging
from mrjob import MRJob

# 初始化 logging 模块
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(name)s %(levelname)s: %(message)s')
logger = logging.getLogger('wc')

class WordCount(MRJob):

    def mapper(self, _, line):
        # 像这样打印中间变量
        logger.info(repr(line))

        yield 'line', 1
        yield 'word', len(line.strip().split())
        yield 'char', len(line)

    def reducer(self, key, values):
        logger.info(repr(key))
        logger.info(repr(values))

        yield key, sum(values)
```

注意：**千万不要随便使用 `print` 语句**，这是 Map-Reduce 计算中的一个禁忌，在调试中尤其应该注意。这是因为 `print` 会默认打印到 stdout 中，这样调试信息就会混淆到数据流中，被 Map-Reduce 所计算。最终你不仅无法看到调试信息，还会污染数据流。而使用 `logging` 模块则不会有这样的问题，因为 `logging` 模块默认输出到 stderr 流。

#### 3.3.4 使用 `merge_output` 选项

Map-Reduce 模型的最大优势就在于其支持大规模并行计算，我们可以使用批量的 mapper 和 reducer 来完成计算。但这也随身附带了一个问题：每个 reducer 都会产生一个输出文件，当 reducer 数量很多时，可能会生成大量细碎的小文件，造成存储集群的 Data-Node 负载过重。mrjob 提供了一个参数 `merge_output` 用于解决这个问题，下面演示其用法：

```python
if __name__ == '__main__':
    job = MyJob()
    job.run(
        merge_output=10,
        # other options ...
        jobconf={
            'mapred.job.reduce.capacity': 1000,
            'mapred.reduce.tasks': 1000,
            # other jobconf ...
        })
```

通过这样的配置，我们既能使用 1000 个 reducer 并行计算，又能保持输出文件只有 10 个。

PS：只有在 `mapred.reduce.tasks` 大于 `merge_output` 时才会触发合并行为。

注意：使用 `merge_output` 选项并不是无代价的，这是因为合并操作是通过一种非常“笨拙”的方法实现的，即构造一个 mapper 为 `cat`、且 `mapred.reduce.tasks=10` 的 hadoop streaming 作业。

## 4. 为什么要使用 mrjob？

为什么要使用 mrjob？用 Python 直接编写 mapper/reducer，然后再调用 hadoop streaming 命令似乎也并不难实现……

在解释原因之前，我们先来看看假如我们不使用 mrjob，而直接调用 hadoop streaming 命令来实现 WordCount，需要怎么做。

首先，我们需要一个 Python 脚本，用于实现 mapper/reducer 函数：

```python
# wc.py
import argparse
import sys

def mapper():
    for line in sys.stdin:
        line = line.rstrip('\n')
        print('line\t1')
        print('word\t' + str(len(line.strip().split())))
        print('char\t' + str(len(line)))

def reducer():
    last_key = None
    sum_ = 0
    for line in sys.stdin:
        key, value = line.rstrip('\n').split('\t', 1)
        if last_key is not None and last_key != key:
            print('{}\t{}'.format(last_key, sum_))
            sum_ = 0
        last_key = key
        sum_ += int(value)
    print('{}\t{}'.format(last_key, sum_))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mapper', dest='mapper', action='store_true')
    parser.add_argument('--reducer', dest='reducer', action='store_true')

    args = parser.parser_args()
    if args.mapper:
        mapper()
        exit(0)
    if args.reducer:
        reducer()
        exit(0)
    raise ValueError('Hey dude, what do you want to run? mapper or reducer?')
```

其次，我们需要一个 shell 脚本作为任务的入口：

```sh
#!/bin/bash
hadoop=/home/zhuhe212/hadoop/bin/hadoop
output=hdfs://localhost:9902/user/zhuhe212/tmp/test_mrjob/

# 这里需要自行清理 output 目录
$hadoop fs -rmr "$output"

$hadoop streaming  \
    -D mapred.job.tracker=jobtracker1.domain:54311  \
    -D mapred.job.map.capacity=4000  \
    -D mapred.job.queue.name=queuename1  \
    -D mapred.job.name=zhuhe212_word_count_by_mrjob  \
    -D mapred.reduce.tasks=1  \
    -D mapred.job.reduce.capacity=800  \
    -D mapred.job.priority=NORMAL  \
    -input hdfs://localhost:9902/user/zhuhe212/tmp/test  \
    -output "$output"  \
    -cacheArchive 'hdfs://localhost:9902/user/zhuhe212/common/python2.7.tar.gz#python2.7.1'  \
    -file wc.py  \
    -mapper 'python2.7.1/python/bin/python "wc.py" --mapper'  \
    -reducer 'python2.7.1/python/bin/python "wc.py" --reducer' \
|| exit 1
```

好了，现在我们用普通方法实现了简单的 WordCount 示例。与 mrjob 版本的实现相比，我们的脚本数量从一个变成了两个，代码量几乎变成了原来的五倍（很大一部分是为了处理不得不处理的异常与用户交互）。尽管如此，我们还尚未获得调用时动态配置参数的功能……

退一步说，即便我们不考虑上述的“闲杂”功能，只看核心的 mapper/reducer 函数，我们依然会发现这个版本的 mapper/reducer 函数充满了令人头疼的底层操作，例如读写 stdin/stdout、切分/拼接字符串、匹配相同的 key 等，可读性很差。

看完了以上的对比，我们现在很容易发现使用 mrjob 具有以下好处：

- 代码高内聚，只需要一个 Python 脚本，不需要额外的配置文件或入口脚本。
- 代码简洁，可读性强。
- 不含底层操作，不容易出错。
- 完备的异常处理，mrjob 不会让任何错误悄悄溜走。
- mapper/combiner/reducer 的输入输出完全 Python 对象化，你可以在三者之间直接传递 Python 对象（例如数字、列表、字典等），而不必手动拼接成 `\t` 连接的字符串。（如果你觉得好奇，这一特性是通过内置的 [pickle](https://docs.python.org/2/library/pickle.html) 库实现的）

### 4.1 为什么不直接使用开源的 mrjob，而要自己实现？

这是一个好问题！但是我用实际经历告诉你，千万不要妄图在我厂的开发机上安装任何涉及到 C 扩展的包，因为安装多少次就会失败多少次……，mrjob 就是一个典型的无法安装的库。

另外，开源库 mrjob 本身也存在很多问题：集成了太多的功能导致代码庞大；用起来比较复杂，因为每个 hadoop 任务都需要写单独的配置文件。

最后，自己编写的库可以针对我厂的工作环境进行定制化设计，比如自动配置可用的任务队列、自动合并输出小文件等。

## 5. 参考手册

### 5.1 各 Runner 类支持的参数

**LocalRunner**

- `input`: 本地输入路径。可以包含通配符；可以使用多个 `-input` 指定多个输入路径；设为 `-` 表示从 stdin 中输入。
- `output`: 本地输出路径。设为 `-` 表示输出到 stdout。
- `mapper`:
- `combiner`:
- `reducer`:

PS: mapper/reducer/combiner 一般不需要设置，Runner 会帮你自动生成。目前还没发现什么场景需要手动设置 mapper/reducer/combiner 参数，但是为了可扩展性还是保留了这三个参数。

**HadoopRunner**

- `hadoop`: hadoop streaming 命令所调用的 hadoop 客户端可执行文件。
- `input`:
- `output`:
- `cmdenv`:
- `cacheArchive`:
- `file`:
- `mapper`:
- `combiner`:
- `reducer`:
- `inputformat`:
- `outputformat`:
- `partitioner`:
- `others`: 用户可自由设置的其他命令或参数，会追加在生成的 hadoop streaming 命令末尾。
- `merge_output`: 将输出目录的文件合并到指定的个数。这是 mrjob 定制的一个功能，用于减少小文件数量。比如你可以指定 `jobconf['mapred.reduce.tasks']=1000`，同时 `merge_output=10`，这样既能保证 reducer 的大并发量（1000），又能使得输出的文件数量较少（10）。

PS: 未做说明的参数，其含义同 hadoop streaming 命令。mapper/reducer/combiner 一般不需要设置，Runner 会帮你自动生成。目前还没发现什么场景需要手动设置 mapper/reducer/combiner 参数，但是为了可扩展性还是保留了这三个参数。
