import curses
import statistics
from slor.shared import *

def average_plot(values:list, width:int=72, trim=1, auto_scale=True, max_x=0, min_x=0):

    values.sort()
    # Get this before trim
    mean = statistics.mean(values)
    median = statistics.median(values)
    # Now trim
    values = values[:round(len(values)*trim)]
    max_x_t = values[-1]
    min_x_t = values[0]
    max_x = max_x_t if max_x_t > max_x else max_x
    min_x = min_x_t if min_x_t < min_x else min_x
    
    if auto_scale:
        # All bets are off if top of range is < 1 
        max_x = autoround(max_x)

    inc=width/max_x
    mean_quant = round(mean*inc)
    min_quant = round(min_x*inc)
    median_quant = round(median*inc)
    grstr = ""
    for i in range(0, width):
        if i == 0:
            grstr += "├"
        elif i == mean_quant:
            grstr += "╋"
        elif i == median_quant:
            grstr += "┿"
        elif i == min_quant:
            grstr += '┍'
        elif i < min_quant:
            grstr += "┄"
        elif i >= min_quant:
            grstr += "━"

    term_char = '┿━>' if trim < 1 else '┥'
    return_str = "{:>11}{}{}".format("mmm", grstr, term_char)
    return  return_str




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
    blocks = (bg, "▁","▂","▃", "▄", "▆", "▆", "▇")
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

    curses_win.addstr('\n')

    block_rez = round(top/height)

    if block_rez < 1:
        return "[histogram not available]"
    
    current_line=top

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

        curses_win.addstr('\n')

        current_line -= block_rez

        if l == height-1:

            curses_win.addstr(" "*11)
            
            vl = len(values)
            for n in [4,2,1]:
                if int(vl/n) == vl/n:
                    strlen = (vl/n)
                    for m in range(0,n):
                        curses_win.addstr("{0:<{1}}".format("│{:.2f}{}".format(values[int((vl/n)*m)]["val"], units), int(strlen)))
                    curses_win.addstr("│")
                    break

    curses_win.noutrefresh()
