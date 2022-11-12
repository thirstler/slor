from slor.shared import *
import time
import math
import sys
import curses
import statistics

class ConsoleToSmall(Exception):
    pass

class SlorScreen:

    stdscr = None
    title_win = None
    progress_win = None
    data_win = None
    footer_win = None
    data_scr_lines = None
    counters = {}
    timer = 0

    def __init__(self, stdscr):

        curses.start_color() 
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLACK)
        curses.init_pair(3, curses.COLOR_CYAN, curses.COLOR_YELLOW)
        curses.init_pair(4, curses.COLOR_BLACK, curses.COLOR_CYAN)
        curses.init_pair(5, curses.COLOR_BLACK, curses.COLOR_MAGENTA)
        curses.init_pair(6, curses.COLOR_CYAN, curses.COLOR_BLACK)
        curses.init_pair(7, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(8, curses.COLOR_RED, curses.COLOR_BLACK)
        self.stdscr = stdscr

        if not self.check_window():
            raise ConsoleToSmall

        self.init_windows()
        self.counters["unkown_progress"] = 0
        self.timer = time.time()
        
    def check_window(self):
        if curses.LINES < TERM_ROW_MIN or curses.COLS < TERM_COL_MIN:
            return False
        return True

    def init_windows(self):
        self.title_win = curses.newwin(1, curses.COLS, 0, 0)
        self.title_win.bkgd(' ', curses.color_pair(1))
        self.progress_win = curses.newwin(1, curses.COLS, 1, 0)
        self.progress_win.bkgd(' ', curses.color_pair(4))
        self.data_win = curses.newwin(curses.LINES-3, curses.COLS, 2, 0)
        self.data_win.bkgd(' ', curses.color_pair(2))
        self.footer_win = curses.newwin(1, curses.COLS, curses.LINES-1, 0)
        self.footer_win.bkgd(' ', curses.color_pair(1))
        self.footer_win.nodelay(True)


    def clear_all(self):
        pass

    def set_footer(self, footer_text):
        self.footer_win.clear()
        self.footer_win.addstr(footer_text)
        self.footer_win.noutrefresh()

    def set_title(self, title, append=False):
        if not append: self.title_win.clear()
        self.title_win.addstr(title)
        self.title_win.noutrefresh()

    def show_bench_table(self, sample, ops:list):

        #self.data_win.noutrefresh()

        ''' Just hard-code this shit, no one cares '''
        self.data_win.addstr("\n ")
        self.data_win.addstr("{:14}".format("operation"))
        self.data_win.addstr("{:12}".format("objects/s"))
        self.data_win.addstr("{:14}".format("bandwidth"))
        self.data_win.addstr("{:12}".format("ms avg"))
        self.data_win.addstr("{:14}".format("99p ms"))
        self.data_win.addstr("{:12}".format("fail/s"))
        self.data_win.addstr("{:10}\n".format("CV"))
        self.data_win.addstr(" "+"─"*90+"\n")

        ios_t = 0
        resp_t = 0
        fail_t = 0
        cv_t = 0

        for op in ops:

            iotime = sample.get_metric("iotime", op)  
            ios = sample.get_rate("ios", op)
            resp = sample.get_resp_avg(op)
            fail = sample.get_metric("failures", op)
            try:
                cv = statistics.stdev(iotime)/statistics.mean(iotime)
            except statistics.StatisticsError:
                cv = 0

            # gather some totals (only relevant if workload is mixed)
            ios_t += ios
            resp_t += resp
            fail_t += fail
            cv_t += cv

            # Warnings for coefficient of variation
            cv_c = curses.color_pair(7) if cv > 0.5 else 0
            cv_c = curses.color_pair(8) if cv > 0.75 else cv_c
            cv_c = curses.color_pair(8)|curses.A_BOLD if cv > 1.0 else cv_c
            fr_c = curses.color_pair(8)|curses.A_BOLD if fail > 0 else 0
            
            self.data_win.addstr(" ")
            self.data_win.addstr("{:<14}".format(op))
            self.data_win.addstr("{:<12}".format(human_readable(ios, print_units="ops")), curses.A_BOLD)
            self.data_win.addstr("{:<14}".format(human_readable(sample.get_rate("bytes", op))+"/s"), curses.A_BOLD)
            self.data_win.addstr("{:<12.2f}".format(resp*1000), curses.A_BOLD)
            self.data_win.addstr("{:<14.2f}".format(sample.get_perc(op, 0.99)*1000))
            self.data_win.addstr("{:<12}".format(human_readable(fail)), fr_c)
            self.data_win.addstr("{:<10}\n".format("{:.4f}".format(cv)), cv_c)

        if len(ops) > 1:
            self.data_win.addstr(" ")
            self.data_win.addstr("{:<14}".format("totals"))
            self.data_win.addstr("{:<12}".format(human_readable(ios_t, print_units="ops")), curses.A_BOLD)
            self.data_win.addstr("{:<14}".format("-"))
            self.data_win.addstr("{:<12.2f}".format((resp_t/len(ops))*1000), curses.A_BOLD)
            self.data_win.addstr("{:<14}".format("-"))
            self.data_win.addstr("{:<12}".format(human_readable(fail_t)))
            self.data_win.addstr("{:<10}\n".format("{:.4f}".format(cv_t/len(ops))), cv_c)


        self.data_win.noutrefresh()


    def set_progress(self, percent:float=None, message=None, final=False):
        
        self.progress_win.clear()

        if percent == None:
            blocks = ("|","/","-","\\")
            self.counters["unkown_progress"]
            self.progress_win.addstr(" ?% {}{}".format(blocks[self.counters["unkown_progress"] % len(blocks)], " "*(curses.COLS-6)))
            self.counters["unkown_progress"] += 1
            self.progress_win.noutrefresh()
            return

        if final: percent = 1
        if percent > 1: percent = 1
        if percent < 0: percent = 0

        fillchar = "█"
        blocks = ("▏","▎","▍", "▌", "▋", "▊", "▉", "█") # eighth blocks
        char_w = (percent * (curses.COLS-2))

        bar_char = fillchar * (math.floor(char_w)) + blocks[math.floor((char_w * 8) % 8)]
        bar_char += " "*int(curses.COLS-char_w-2)
        perc_bh = math.ceil(percent * 100)
        if len(bar_char) < 5:
            self.progress_win.addstr(bar_char)
            self.progress_win.addstr(str(perc_bh)+"%")
        else:
            self.progress_win.addstr("{:>3}%".format(perc_bh))
            self.progress_win.addstr(bar_char[:-4], curses.color_pair(6))

        self.progress_win.noutrefresh()

    def set_message(self, message=None):
        self.progress_win.clear()
        self.progress_win.addstr(message)
        self.progress_win.noutrefresh()

    def clear_message(self):
        self.progress_win.clear()
        self.progress_win.noutrefresh()

    def clear_progress(self):
        self.progress_win.clear()
        self.progress_win.noutrefresh()

    def set_data(self, data_str, append=False):
        if append:
            if self.data_scr_lines == None: self.data_scr_lines = []
            self.data_scr_lines += data_str.splitlines()
        else:
            self.data_scr_lines = data_str.splitlines()

        avail_lines = curses.LINES-3

        if len(self.data_scr_lines) > avail_lines:
            data_str = '...\n' + '\n'.join(self.data_scr_lines[-avail_lines:])
        else:
            data_str = '\n'.join(self.data_scr_lines)  

        self.data_win.clear()
        try:
            self.data_win.addstr(data_str)
        except:
            sys.stderr.write("{}:{}\n".format(-avail_lines, len(self.data_scr_lines)))
            sys.stderr.flush()
            time.sleep(30)

        self.data_win.noutrefresh()

    def clear_data(self):
        self.data_win.clear()
        self.data_scr_lines.clear()
        self.progress_win.noutrefresh()

    def refresh(self):
        # Put the brakes on the refresh rate in case something goes nuts
        nownow = time.time()
        if (nownow - self.timer) > MAX_CURSES_REFRESH_RATE:
            curses.doupdate()
            self.timer = nownow

    def display_error(self, message):
        win = curses.newwin(curses.LINES, curses.COLS, 0, 0)
        win.addstr(message)
        win.refresh()
        time.sleep(2)

