from slor.shared import *
import curses
import statistics
import time

def average_plot(values:list, width:int=72, trim=1, auto_scale=True, max_x=-1, min_x=-1, plot=True):

    values.sort()
    # Get this before trim
    mean = statistics.mean(values)
    median = statistics.median(values)

    # Now trim
    values = values[:round(len(values)*trim)]
    max_now = values[-1]
    min_now = values[0]
    max_x = values[-1] if max_x == -1 else max_x
    min_x= values[0] if min_x == -1 else min_x
    
    if auto_scale:
        # All bets are off if top of range is < 1 
        max_x = autoround(max_x)

    inc=width/max_x
    mean_quant = round(mean*inc)
    min_quant = round(min_now*inc)
    max_quant = round(max_now*inc)
    median_quant = round(median*inc)

    grstr = ""
    if plot:
        for i in range(0, width):

            if i == min_quant:
                grstr += '┶'
            elif i <= max_quant and i == (width-1):
                grstr += '┵'
            elif i == max_quant:
                grstr += '┵'
            elif i == mean_quant:
                grstr += "╋"
            elif i == median_quant:
                grstr += "┿"
            elif i > min_quant and i < max_quant:
                grstr += "━"
            else:
                grstr += "┄"
        
    term_char = '┄┄>' if trim < 1 else '┥'
    return_str = "{:>11}{}{}\n".format("", grstr, term_char)
    return_str += "{:>11} min: {:.2f}ms|median: {:.2f}ms|mean: {:.2f}ms|max: {:.2f}\n".format(
        "", min_now, median, mean, max_now
    )
    return  return_str



def io_history(values:dict, curses_win, min_y=-1, max_y=-1, width=72, height=5, sample_density=1, bg=" ", config=None, stage_class=None, ready=True):
    time_range = width*sample_density
    blocks = (bg, "▁","▂","▃", "▄", "▅", "▆", "▇")
    start_sec = int(values[0]["ts"])

    if ready:
        plot_color = curses.color_pair(6)
    else:
        plot_color = curses.color_pair(7)

    peak = max(map(lambda x:x['ios'], values))

    if len(values) >= time_range:
        values = values[-time_range+1:]
        
    tsb = int(values[0]["ts"])
    slot = lambda x:int(((x-tsb)/time_range)*width)

    # Build an empty matrix
    graph_matrix = []
    for q in range(tsb, tsb+time_range, sample_density):
        graph_matrix.append([q, 0])

    max_y = 0
    # Populate the matrix
    for v in values:

        i = slot(v["ts"])

        # Sanity check
        if i >= len(graph_matrix):
            i = len(graph_matrix)-1
        elif i < 0:
            i = 0

        graph_matrix[i][1] += v["ios"]
        if graph_matrix[i][1] > max_y:
            max_y = graph_matrix[i][1]

    # Anti-shift the matrix
    for n, v in enumerate(graph_matrix):
        if n == len(graph_matrix)-1 or n == 0:
            continue # don't process the first/last value
        if graph_matrix[n][1] == 0 and graph_matrix[n-1][1] > 0 and graph_matrix[n+1][1] > 0:
            graph_matrix[n][1] = round((graph_matrix[n-1][1]+graph_matrix[n+1][1])/2)

        
    peak = autoround(peak)
    hrez = peak/height
    end_point = int(config["run_time"]/sample_density)+1+DRIVER_REPORT_TIMER
    if end_point >= width or stage_class in PROGRESS_BY_COUNT:
        end_point = -1
    curses_win.addstr(" IO History╭")
    curses_win.addstr("─"*72 + "╮\n")
    for r in range(height, 0, -1):
        current_top = r * hrez
        curses_win.addstr("{:>11}│".format(int(current_top)))
        for c in range(0, width):
            if graph_matrix[c][1] >= current_top:
                curses_win.addstr("█", plot_color)
            elif graph_matrix[c][1] > (current_top-hrez):
                curses_win.addstr(blocks[int(((graph_matrix[c][1] % hrez)/hrez)*len(blocks))], plot_color)
            elif c == end_point:
                curses_win.addstr("│", curses.A_DIM)
            else:
                curses_win.addstr(bg)
        curses_win.addstr('│\n')
    gr = "{:>11}│".format("seconds")

    sec_z = int(values[0]["ts"]) - start_sec
    for n in range(sec_z+DRIVER_REPORT_TIMER, width+sec_z, 6):
        gr += "{:<6}".format(n)
        n += sample_density*6
    curses_win.addstr(gr+'\n')

            
            

def histogram_data(values:list, partitions:int, auto_scale=True, trim=1, max_x=0, min_x=0) -> dict:

    values.sort()
    values = values[:round(len(values)*trim)]
    
    max_x_t = max(values)
    max_x = max_x_t if max_x_t > max_x else max_x
    
    if auto_scale:
        # All bets are off if top of range is < 1 
        max_x = autoround(max_x)
        min_x = autoround(min_x, down=True)

    d_range = max_x-min_x
    resolution = d_range/partitions
    slot = lambda x:int((x-min_x)/resolution)
    hist_data = []
    ticker = min_x
    for h in range(0, partitions):
        hist_data.append({"val": ticker, "count": 0})
        ticker += resolution
    for i in values:
        s = (partitions-1) if i == max_x else slot(i)
        hist_data[s]["count"] += 1
    return(hist_data)

def histogram_graph_str(values:dict, height:int=10, max_y=0, min_y=0, label:str=None, bg=" ", units=""):
    """
    Output to a string
    """
    blocks = (bg, "▁","▂","▃", "▄", "▅", "▆", "▇")
    text = ""

    if max_y > 0:
        top = autoround(max_y)
    else:
        top = autoround(max(values, key=lambda x:x['count'])['count'])

    opcount = 0
    for bin in values:
        opcount += bin["count"]

    block_rez = round(top/height)

    if block_rez < 1:
        return "[histogram not available]"
    
    current_line=top

    for l in range(0, height):

        text += "{:>11}│".format(human_readable(round(current_line), print_units="ops"))

        for col in values:

            if col["count"] >= current_line:
                tb = "█"
            elif col["count"] > (current_line-block_rez):
                tb = blocks[int(((col["count"] % block_rez)/block_rez)*len(blocks))]
            else:
                tb = bg
            
            text += tb

        text += '\n'

        current_line -= block_rez

        if l == height-1:

            text += " "*11
            
            vl = len(values)
            for n in [4,2,1]:
                if int(vl/n) == vl/n:
                    strlen = (vl/n)
                    for m in range(0,n):
                        text += "{0:<{1}}".format("│{:.2f}{}".format(values[int((vl/n)*m)]["val"], units), int(strlen))
                    text += "│"
                    break

    return text


def histogram_graph_curses(values:dict, curses_win, height:int=10, max_y=0, min_y=0, label:str=None, bg=" ", units=""):
    """
    Output to an ncurses window 
    """

    blocks = (bg, "▁","▂","▃", "▄", "▆", "▆", "▇")

    if max_y > 0:
        top = autoround(max_y)
    else:
        top = autoround(max(values, key=lambda x:x['count'])['count'])

    opcount = 0
    for bin in values:
        opcount += bin["count"]

    block_rez = round(top/height)

    if block_rez < 1:
        return "[histogram not available]"
 
    current_line=top
    curses_win.addstr(" operation time distribution (99 percentile) ")
    curses_win.addstr("─"*39 + "╮\n")
    for l in range(0, height):

        curses_win.addstr("{:>11}│".format(human_readable(round(current_line), print_units="ops")))

        for col in values:

            if col["count"] >= current_line:
                tb = "█"
            elif col["count"] > (current_line-block_rez):
                tb = blocks[int(((col["count"] % block_rez)/block_rez)*len(blocks))]
            else:
                tb = bg
            
            curses_win.addstr(tb, curses.color_pair(6))

        curses_win.addstr('│\n')

        current_line -= block_rez

        if l == height-1:

            curses_win.addstr(" "*11)
            
            vl = len(values)
            for n in [4,2,1]:
                if int(vl/n) == vl/n:
                    strlen = (vl/n)
                    for m in range(0,n):
                        curses_win.addstr("{0:<{1}}".format("│{:.2f}{}".format(values[int((vl/n)*m)]["val"], units), int(strlen)))
                    break
            

    curses_win.noutrefresh()
