import numpy as np
import matplotlib.pyplot as plt

from matplotlib.colors import BoundaryNorm, ListedColormap

def plot_da_categories(da, vcls, title=None, cb=True, **kwargs):
    """
    Plot categorical data with optional colorbar and title.

    Parameters:
    ----------
    da : xarray.DataArray
        The data array containing the categorical data to be plotted.
    vcls : list of tuples
        A list of tuples where each tuple contains (value, color, label).
        - value: The categorical value.
        - color: The RGB color as a tuple of integers (0-255).
        - label: The label for the category.
    title : str, optional
        The title of the plot. If None, the time value of the data array will be used as the title.
    cb : bool, optional
        Whether to display the colorbar. Default is True.
    **kwargs : dict
        Additional keyword arguments to pass to plt.subplots.
    """

    ticks, colors, labels = zip(*vcls)
    cmap = ListedColormap([np.array(color) / 255 for color in colors])
    value_to_index = {value: index for index, value in enumerate(ticks)}
    mapped_data = np.vectorize(value_to_index.get)(da)
    norm = BoundaryNorm(boundaries=np.arange(-0.5, len(ticks), 1), ncolors=cmap.N, clip=True)

    # Plot the data using matplotlib's pcolormesh
    fig, ax = plt.subplots(**kwargs)
    im = ax.pcolormesh(da.x, da.y, mapped_data, cmap=cmap, norm=norm)
    cbar = fig.colorbar(im, ax=ax, ticks=np.arange(len(ticks)))
    cbar.ax.set_yticklabels(labels)

    if not cb:
        # Hide the colorbar completely
        cbar.ax.set_visible(False)

    if title is None:
        plt.title(da.time.values)
    else:
        plt.title(title)
    plt.gca().set_aspect('equal')
    plt.show()