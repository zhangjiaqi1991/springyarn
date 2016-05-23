/**
 * Created by hy on 16-5-11.
 */
if (fsh.test(indir)) {
    fsh.rmr(indir);
}
if (fsh.test(outdir)) {
    fsh.rmr(outdir);
}
fsh.copyFromLocal('/home/hy/'+source+'/'+file, indir+'/'+file);