/ 2011.09.26 - handle nested column #_file correctly 
/ see https://code.kx.com/trac/wiki/Cookbook/FileCompression
td:`:/Volumes/Stuff/tmp
td:`:/tmp
t:`trade
d:`:/Volumes/Stuff/NYSETAQ.2010/taq; p:2010.08.06
d:`:/Volumes/Stuff/NYSETAQ.2007/taq; p:2007.05.04
d:`:/Volumes/Stuff/NYSETAQ.2003/taq; p:2003.09.10

/ info table::
/ ac - actual compression %age
/ algo - algorithm 
/ blksz - blksize 
/ cl - compressed length
/ ec - estimated compression %age based on a quick -18! of a chunk of the file (conservative)
/ lvl - level used (only available on cwrite, not part of disk meta-info)
/ name - column(/file) name in table
/ ok - compression has been checked by comparing valueBefore ~ valueAfter
/ ptn - partition name
/ rw - column should be rewritten, this has to be set if a file is to be compressed or mv'd 
/ sf - source file
/ tbl - table name 
/ tf - target file 
/ time - time taken to compress
/ ucl - uncompressed length

/ LASTINFO - last changed copy of <info>, kept around as building it can be very expensive (cwrite/cvalidate)

\g 1 

n21:{[f;k;dv]$[count v:(-21!f)k;v;dv]}
n21cl:n21[;`compressedLength;0j]
n21ucl:n21[;`uncompressedLength;0j]
n21a:n21[;`algorithm;0N]
n21lbs:n21[;`logicalBlockSize;0N]

/ cinfo:: d - db directory; td - target db directory (ideally on a different physical drive); p - partition; t - tablename
cinfo:{[d;td;p;t] dpt:.Q.par[d;p;t]; tdpt:.Q.par[td;p;t]; c:(key dpt)except`.d; 
    r:([]sf:(` sv)each(dpt,)each c;tf:(` sv)each(tdpt,)each c);
    r:update ptn:p,tbl:t,name:c,rname:c,id:-1,cl:n21cl each sf,ucl:n21ucl each sf,algo:n21a each sf,blksz:n21lbs each sf from r;
    r:update ucl:hcount each sf from r where ucl=0; r:update cl:ucl from r where cl=0;
    r:update ec:ac from update time:`time$0,lvl:0N,ok:1b,rw:0b,ac:100*1-cl%ucl from r;
    r:update rname:{`$-1_x}each string name from r where name like"*#"; / root name from name, xxx from xxx#
    r:update rw:1b,ec:{max 1,100*1-(count -18!v)%count -8!v:read1(x;0;500000)}each sf from r where ac=0;
    :LASTINFO::`ptn`tbl`name`rname`ok`rw`ac`ec`id`time`cl`ucl`algo`blksz`lvl xcols r} 

ctotal:{[info] 
    / exec ucl wavg ec,ucl wavg ac,sum cl,sum ucl,sum time from info}
    0!select ucl wavg ec,ucl wavg ac,sum cl,sum ucl,sum time by ptn,tbl,algo,lvl from info}

cwrite:{[info]
    if[not all exec(blksz within 12 20)and((algo in 0 1)and lvl=0)or(algo=2)and lvl within 1 9 from info where rw;'"invalid blksz/algo/lvl"];
    r:update ok:0b,tmp:{[sf;tf;b;a;l] t:.z.t;r:(.z.t-t;-19!(sf;tf;b;a;l));-1(string first r)," ",1_string tf;r}'[sf;tf;blksz;algo;lvl]from info where rw;
    :LASTINFO::delete tmp from update cl:n21cl each tf,time:first each tmp,ac:last each tmp from r where rw}

cuse:{[info;blksZ;algO;lvL] / update the -19! parameters to be used where rw=1b
    / make sure matching pairs of xxx & xxx# within ptn/tbl
    r:update id:{x?x:flip x}(ptn;tbl;rname) from info;
    r:update rw:1b from r where id in exec id from r where rw;
    :LASTINFO::update blksz:blksZ,algo:algO,lvl:lvL from r where rw}
cusegzl:cuse[;17;2;] / 128K, gzip
cusegz:cusegz6:cusegzl[;6] / gzip, level=6, ZFS default
cusegz1:cusegzl[;1] / gzip, level=1, surprisingly good
cusegz9:cusegzl[;9] / gzip, level=9, maximum
cuselogfile:cuse[;20;2;9] / biggest blocksize, gzip, level=9, maximum
cusekx:cuse[;17;1;0] / 128K, kx

conlymv:{[pct;info] 
    / only mv those columns which have >pct% compression
    :LASTINFO::update rw:ac>pct from info where rw}
conlymv65:conlymv 65

cvalidate:{[info] / make sure the compression worked, only set ok those that did compress
    r:update ok:{$[hcount[x]~hcount y;$[(read1(x;0;4000))~read1(y;0;4000);(get x)~get y;0b];0b]}'[sf;tf]from info where ac>0,rw,name=rname,not ok;
    :LASTINFO::update ok:1b from r where ac>0,rw,name<>rname,id in exec id from r where ok}

cokmv:{[info] / all validated before the mv?
    exec all ok from info where ac>0,rw}

cshowmv:{[info] / use output from this to build a mv script 
    exec{-1"mv ",(1_string x)," ",1_string y;}'[tf;sf]from info where ac>0,rw;}

cmv:{[info] / mv the files (\r isn't as flexible at moving across filesystems)  
    if[r:cokmv info; / don't skip this check! once the data's been mv'd the original is irretrievably GONE
        /exec{-1 r:"r ",(1_string x)," ",1_string y;system r;}'[tf;sf]from info where ac>0,rw];
        exec{-1 r:"mv ",(1_string x)," ",1_string y;system r;}'[tf;sf]from info where ac>0,rw];
    r}

/ reload results if saved as a csv 
loadcsv:0:[("DSSSBBEEHTJJHHHSS";enlist",")]

\
sample session:
info:cinfo[d;td;p;t]
info / inspect data, adjust <rw> setting
info:cusekx info / set kx compression as the one to be used for all 
info:cwrite info / do the compression
info / inspect results, perhaps rack up the level for some columns and rewrite 
cmv info:cvalidate info / make sure all ok, then mv 
or:
cmv conlymv65 info:cvalidate info / make sure ok, then move all with >75% compression only 
or:
cmv conlymv65 cvalidate cwrite cusegz6 cinfo[d;td;p;t]
====
save`:info.csv
.. another session ..
info:loadcsv`:info.csv
====
build a bulk request by appending to info:
info:cinfo[d;td;p;`trade]
info,:cinfo[d;td;p;`quote]
info,:cinfo[d;td;p+1;`trade]
info,:cinfo[d;td;p+1;`quote]
..                 
info:raze cinfo[d;td;p;]each `trade`quote`nbbo
don't bulk up too much! it's a lot of data..
