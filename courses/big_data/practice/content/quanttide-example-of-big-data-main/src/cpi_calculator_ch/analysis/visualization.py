import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import logging
from typing import Optional
from datetime import datetime
from matplotlib import rcParams
import platform


class PriceIndexVisualizer:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.logger = logging.getLogger('visualization')
        self._set_chinese_font()

    def _set_chinese_font(self):
        """配置中文字体支持"""
        system = platform.system()
        font_map = {
            'Windows': 'SimHei',
            'Darwin': 'Arial Unicode MS',
            'Linux': 'WenQuanYi Zen Hei'
        }
        rcParams['font.sans-serif'] = [font_map.get(system, 'SimHei')]
        rcParams['axes.unicode_minus'] = False

    def load_index_data(self, filename: str) -> Optional[pd.DataFrame]:
        """从CSV文件加载指数数据"""
        filepath = self.data_dir / filename
        try:
            df = pd.read_csv(filepath)
            df['date'] = pd.to_datetime(df['date'])
            if 'base_date' in df.columns:
                df['base_date'] = pd.to_datetime(df['base_date'])
            return df
        except Exception as e:
            self.logger.error(f"加载指数文件失败 {filename}: {str(e)}")
            return None

    def plot_single_index(self, df: pd.DataFrame, title: str, save_path: Optional[str] = None):
        """绘制单个指数曲线"""
        plt.figure(figsize=(12, 6))
        plt.plot(df['date'], df['index'], 'b-', linewidth=2, label='价格指数')

        base_date = pd.to_datetime(df['base_date'].iloc[0])
        base_value = df[df['date'] == base_date]['index'].values[0]

        plt.scatter(
            base_date,
            base_value,
            color='r',
            s=100,
            label=f'基期 ({base_date.strftime("%Y-%m-%d")})'
        )

        plt.title(f'{title}\n(基期: {base_date.strftime("%Y-%m-%d")}, 指数={base_value})')
        plt.xlabel('日期')
        plt.ylabel('价格指数')
        plt.grid(True)
        plt.legend()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"已保存图表到: {save_path}")
        plt.close()

    def plot_dual_indices(self, cavallo_df: pd.DataFrame, tmall_df: pd.DataFrame, save_path: Optional[str] = None):
        """绘制双指数对比曲线"""
        plt.figure(figsize=(14, 7))

        plt.plot(cavallo_df['date'], cavallo_df['index'], 'b-', linewidth=2, label='Cavallo指数')
        plt.plot(tmall_df['date'], tmall_df['index'], 'g--', linewidth=2, label='Tmall指数')

        base_date = pd.to_datetime(cavallo_df['base_date'].iloc[0])
        plt.axvline(
            x=base_date,
            color='r',
            linestyle=':',
            label=f'共同基期 ({base_date.strftime("%Y-%m-%d")})'
        )

        plt.title('价格指数对比\n(基期: {})'.format(base_date.strftime("%Y-%m-%d")))
        plt.xlabel('日期')
        plt.ylabel('价格指数')
        plt.grid(True)
        plt.legend()

        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            self.logger.info(f"已保存对比图表到: {save_path}")
        plt.close()

    def visualize_all(self):
        """可视化所有指数数据"""
        try:
            cavallo = self.load_index_data("cavallo_index.csv")
            tmall = self.load_index_data("tmall_index.csv")

            if cavallo is None or tmall is None:
                raise ValueError("无法加载指数数据文件")

            self.plot_single_index(cavallo, "Cavallo价格指数", self.data_dir / "cavallo_index.png")
            self.plot_single_index(tmall, "Tmall价格指数", self.data_dir / "tmall_index.png")
            self.plot_dual_indices(cavallo, tmall, self.data_dir / "price_indices_comparison.png")

        except Exception as e:
            self.logger.error(f"可视化失败: {str(e)}", exc_info=True)
            raise