/**
 * Created by hy on 16-5-11.
 */
if (fsh.test(indir)) {
    fsh.rmr(indir);
}
if (fsh.test(outdir)) {
    fsh.rmr(outdir);
}
fsh.copyFromLocal('/Users/zjq/IdeaProjects/springyarn'+source+'/'+file, indir+'/'+file);