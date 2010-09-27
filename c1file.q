o:.Q.opt .z.x;if[not 2=count .Q.x;-2"usage: q ",(string .z.f)," SOURCE TARGET [-blksz NN(default 17)] [-logfile] [-algo N(default 2)] [-level N(default 6)] [-exit]\n";exit 64]
/ SOURCE - source filename
/ TARGET - target filename
/ -logfile - use gzip, level 9, max blksz unless explicitly set
/ -blksz, -algo, -level see -19! documentation
/ -exit - exit on completion. ERRORLEVEL 0 if successful, 1 if validation failed, 64 if invalid usage, 65 if invalid parameters

SOURCE:hsym`${x[where"\\"=x]:"/";x}.Q.x 0
TARGET:hsym`${x[where"\\"=x]:"/";x}.Q.x 1
BLKSZ:17;ALGO:2;LEVEL:6;
LOGFILE:`logfile in key o

if[`blksz in key o;BLKSZ:first"I"$o`blksz]
if[`algo in key o;ALGO:first"I"$o`algo]
if[`level in key o;LEVEL:first"I"$o`level]

ts:{(string .z.Z)," ",x}
errorunless:{[msg;ok] if[not ok;-2 ts msg;exit 65];}
/ errorunless["already compressed";0=count -21!SOURCE]

if[LOGFILE;
	if[not`blksz in key o;BLKSZ::20];
	if[not`algo in key o;ALGO::2];
	if[not`level in key o;LEVEL::9]]

errorunless["invalid blksz (12-20)";BLKSZ in 12_til 21]
errorunless["invalid algo (0/1/2)";ALGO in 0 1 2]
errorunless["invalid compression level (0 for 0,1; 1-9 for 2)";((ALGO in 0 1)&LEVEL=0)|(ALGO=2)&LEVEL in 1_til 10]

-1 ts"compress ",(1_string SOURCE)," with blksz:",(string BLKSZ)," using algo:",(string ALGO)," at level:",string LEVEL;
$[cpct:floor 0.5+-19!(SOURCE;TARGET;BLKSZ;ALGO;LEVEL);
	-1 ts"saved as ",(1_string TARGET)," with ",(string cpct),"% compression (",(1_raze"/",'string(-21!TARGET)`compressedLength`uncompressedLength),")";
	-1 ts"saved as ",(1_string TARGET)," with NO compression (",(string hcount TARGET),")"];
if[failed:not{$[hcount[x]~hcount y;$[(read1(x;0;4000))~read1(y;0;4000);(read1 x)~read1 y;0b];0b]}[SOURCE;TARGET];
	-2 ts"validation FAILED"]
if[`exit in key o;exit 0+failed]
