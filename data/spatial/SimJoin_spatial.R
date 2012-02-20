as.sdbRegion <-
function (id, x, y) 
{
    region <- c()
    region$id <- as.numeric(id)
    region$x <- as.numeric(x)
    region$y <- as.numeric(y)
    structure(region, class = "sdbRegion")
}
as.sdbRegionList <-
function(x) { structure(x, class = c("sdbRegionList", "list")) }
plot.sdbRegion <-
function(region, ...)
{
    polygon(region$x, region$y, ...)
}
plot.sdbRegionList <-
function (regionList, main = "", sub = "", xlim = NULL, ylim = NULL, 
    axes = T, ann = par("ann"), col = par("col"), ...) 
{
    if (is.null(xlim)) {
        rx <- sapply(regionList, function(region) {
            range(region$x)
        })
        xlim <- range(rx)
    }

    if (is.null(ylim)) {
        ry <- sapply(regionList, function(region) {
            range(region$y)
        })
        ylim <- range(ry)
    }

    opar <- par(no.readonly=T)
    on.exit(par(opar))
    plot.new()
    plot.window(xlim, ylim, ...)
    for (region in regionList)
        plot(region)
    if (axes) {
        axis(1)
        axis(2)
        box()
    }
    if (ann)
        title(main=main, sub=sub, xlab="x", ylab="y", ...)
}
read.sdbRegionList <-
function (file) 
{
    lines <- readLines(file)
    regions <- vector("list", length(lines))
    for (i in 1:length(lines)) {
        con <- textConnection(lines[i])
        id <- scan(con, sep = ",", what = numeric(), nmax = 1, 
            quiet = T)
        n <- scan(con, sep = ",", what = integer(), nmax = 1, 
            quiet = T)
        xy <- scan(con, sep = ",", what = double(), quiet = T)
        x <- xy[1:n * 2 - 1]
        y <- xy[1:n * 2]
        regions[[i]] <- as.sdbRegion(id, x, y)
    }
    structure(regions, class = c("sdbRegionList", "list"))
}
