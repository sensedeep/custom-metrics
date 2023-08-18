module.exports = async () => {
    let pid = parseInt(process.env.DYNAMODB_PID || '')
    if (pid) {
        process.kill(pid)
    }
}
