(ns clojure-hadoop.imports
  ;;#^{:doc "Functions to import entire packages under org.apache.hadoop."}
  )

(defn import-io
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.io into the current namespace."
  []
  (import '(org.apache.hadoop.io
            RawComparator
            SequenceFile$Sorter$RawKeyValueIterator
            SequenceFile$ValueBytes
            Stringifier
            Writable
            WritableComparable
            WritableFactory
            AbstractMapWritable
            ArrayFile
            ArrayFile$Reader
            ArrayFile$Writer
            ArrayWritable
            BooleanWritable
            BooleanWritable$Comparator
            BytesWritable
            BytesWritable$Comparator
            ByteWritable
            ByteWritable$Comparator
            CompressedWritable
            DataInputBuffer
            DataOutputBuffer
            DefaultStringifier
            DoubleWritable
            DoubleWritable$Comparator
            FloatWritable
            FloatWritable$Comparator
            GenericWritable
            InputBuffer
            LongWritable
            LongWritable$Comparator
            IOUtils
            IOUtils$NullOutputStream
            LongWritable
            LongWritable$Comparator
            LongWritable$DecreasingComparator
            MapFile
            MapFile$Reader
            MapFile$Writer
            MapWritable
            MD5Hash
            MD5Hash$Comparator
            NullWritable
            NullWritable$Comparator
            ObjectWritable
            OutputBuffer
            SequenceFile
            SequenceFile$Metadata
            SequenceFile$Reader
            SequenceFile$Sorter
            SequenceFile$Writer
            SetFile
            SetFile$Reader
            SetFile$Writer
            SortedMapWritable
            Text
            Text$Comparator
            TwoDArrayWritable
            UTF8
            UTF8$Comparator
            VersionedWritable
            VLongWritable
            VLongWritable
            WritableComparator
            WritableFactories
            WritableName
            WritableUtils
            SequenceFile$CompressionType
            MultipleIOException
            VersionMismatchException)))

(defn
  import-io-compress
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.io.compress into the current namespace."
  []
  (import '(org.apache.hadoop.io.compress
            CompressionCodec
            Compressor
            Decompressor
            BlockCompressorStream
            BlockDecompressorStream
            CodecPool
            CompressionCodecFactory
            CompressionInputStream
            CompressionOutputStream
            CompressorStream
            DecompressorStream
            DefaultCodec
            GzipCodec
            GzipCodec$GzipInputStream
            GzipCodec$GzipOutputStream)))

(defn
  import-fs
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.fs into the current namespace."
  []
  (import '(org.apache.hadoop.fs
            PathFilter
            PositionedReadable
            Seekable
            Syncable
            BlockLocation
            BufferedFSInputStream
            ChecksumFileSystem
            ContentSummary
            DF
            DU
            FileStatus
            FileSystem
            FileSystem$Statistics
            FileUtil
            FileUtil$HardLink
            FilterFileSystem
            FSDataInputStream
            FSDataOutputStream
            FSInputChecker
            FSInputStream
            FSOutputSummer
            FsShell
            FsUrlStreamHandlerFactory
            HarFileSystem
            InMemoryFileSystem
            LocalDirAllocator
            LocalFileSystem
            Path
            RawLocalFileSystem
            Trash ChecksumException FSError)))

(defn import-mapreduce
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.mapreduce into the current namespace."
  []
  (import '(org.apache.hadoop.mapreduce
            CounterGroup
            Counters
            ID
            InputFormat
            InputSplit
            Job
            JobContext
            JobID
            MapContext
            Mapper
            OutputCommitter
            OutputFormat
            Partitioner
            RecordReader
            RecordWriter
            ReduceContext
            Reducer
            StatusReporter
            TaskAttemptContext
            TaskAttemptID
            TaskID
            TaskInputOutputContext
            InputFormat)))

(defn import-mapreduce-lib
  "Imports all classes/interfaces/exceptions from the package
  org.apache.hadoop.mapreduce.lib into the current namespace."
  []
  (import '(org.apache.hadoop.mapreduce.lib.input
            FileInputFormat
            FileSplit
            InvalidInputException
            LineRecordReader
            SequenceFileInputFormat
            SequenceFileRecordReader
            TextInputFormat)
          '(org.apache.hadoop.mapreduce.lib.map
            InverseMapper
            MultithreadedMapper
            TokenCounterMapper)
          '(org.apache.hadoop.mapreduce.lib.output
            FileOutputCommitter
            FileOutputFormat
            NullOutputFormat
            SequenceFileOutputFormat
            TextOutputFormat)
          '(org.apache.hadoop.mapreduce.lib.partition
            HashPartitioner)
          '(org.apache.hadoop.mapreduce.lib.reduce
            IntSumReducer
            LongSumReducer)))
