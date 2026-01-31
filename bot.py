# Главная функция
def main():
    app = Application.builder().token("8134773553:AAF4DWLR7DBDolkigso_ZgXd4Ml_90YaaK8").build()
    
    # Обработчик теста
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('test', test_command),
            CallbackQueryHandler(level_selected, pattern='^level_')
        ],
        states={
            CHOOSING_LEVEL: [CallbackQueryHandler(level_selected)],
            ANSWERING: [MessageHandler(filters.TEXT & ~filters.COMMAND, answer)]
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CallbackQueryHandler(back_to_main, pattern='^back_to_main$'),
            CallbackQueryHandler(button_handler, pattern='^(about|start_test|leaderboard|my_stats)$'),
            CallbackQueryHandler(button_handler, pattern=r'^leaderboard_page_\d+$')
        ],
        allow_reentry=True
    )
    
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("leaderboard", leaderboard_command))
    
    # ИСПРАВЛЕНО: добавлен r перед строкой
    app.add_handler(CallbackQueryHandler(button_handler, pattern=r'^(about|start_test|leaderboard|my_stats|leaderboard_page_\d+)$'))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern='^back_to_main$'))
    
    print('🤖 Библейский тест-бот запущен!')
    app.run_polling()

if __name__ == '__main__':
    main()
