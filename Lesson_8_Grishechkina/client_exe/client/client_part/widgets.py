from PyQt5 import QtCore, QtWidgets


class QMyTextEdit(QtWidgets.QTextEdit):
    keyPressed = QtCore.pyqtSignal(int)

    def keyPressEvent(self, event):
        super(QMyTextEdit, self).keyPressEvent(event)
        if event.modifiers() == QtCore.Qt.NoModifier:
            self.keyPressed.emit(event.key())
